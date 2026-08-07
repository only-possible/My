package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/gorilla/websocket"
	"github.com/nyaruka/phonenumbers"
	"go.mongodb.org/mongo-driver/bson"
)

// ── XML Structs for Direct OpenXML/Zip Extraction ─────────────────────────────

type SharedStringsXML struct {
	XMLName xml.Name `xml:"sst"`
	SI      []struct {
		T string `xml:"t"`
	} `xml:"si"`
}

type WorksheetXML struct {
	XMLName   xml.Name `xml:"worksheet"`
	SheetData struct {
		Row []struct {
			R string `xml:"r,attr"`
			C []struct {
				R string `xml:"r,attr"`
				T string `xml:"t,attr"`
				V string `xml:"v"`
			} `xml:"c"`
		} `xml:"row"`
	} `xml:"sheetData"`
}

type IvasSMSPayload struct {
	Originator string `json:"originator"`
	Recipient  string `json:"recipient"`
	Message    string `json:"message"`
	Range      string `json:"range"`
}


func saveIvasNumbersFile(countryName string, fileContent string) (string, int, error) {
	cleanName := cleanCountryName(countryName)
	fileName := cleanName + "0.txt"
	filePath := filepath.Join(FilesDir, fileName)

	lines := strings.Split(fileContent, "\n")
	var validNums []string
	for _, l := range lines {
		n := strings.TrimSpace(l)
		if n != "" {
			validNums = append(validNums, n)
		}
	}

	if len(validNums) == 0 {
		return "", 0, fmt.Errorf("file contains no valid phone numbers")
	}

	err := os.WriteFile(filePath, []byte(strings.Join(validNums, "\n")), 0644)
	if err != nil {
		return "", 0, err
	}

	ramNumbersMu.Lock()
	ramNumbers[fileName] = validNums
	ramNumbersMu.Unlock()

	return fileName, len(validNums), nil
}

// ── Background Worker for IVAS WebSocket ──────────────────────────────────────

func startIvasWorker(bot *tgbotapi.BotAPI) {
	cleanURL := strings.TrimSpace(IvasSocketURL)
	if cleanURL == "" || strings.EqualFold(cleanURL, "none") {
		log.Println("⚠️ [IVAS] Socket URL is set to 'None'. IVAS background worker is disabled.")
		return
	}

	go func() {
		botLink := "https://t.me/" + bot.Self.UserName

		for {
			log.Println("🔄 [IVAS] Connecting to WebSocket...")

			tlsConfig := &tls.Config{InsecureSkipVerify: true}
			dialer := websocket.Dialer{TLSClientConfig: tlsConfig}

			conn, _, err := dialer.Dial(IvasSocketURL, nil)
			if err != nil {
				log.Printf("❌ [IVAS] Connection failed: %v. Retrying in 5s...", err)
				time.Sleep(5 * time.Second)
				continue
			}

			log.Println("✅ [IVAS] Connected successfully!")

			_ = conn.WriteMessage(websocket.TextMessage, []byte("40/livesms,"))

			done := make(chan struct{})
			go func() {
				ticker := time.NewTicker(20 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						if err := conn.WriteMessage(websocket.TextMessage, []byte("3")); err != nil {
							return
						}
					case <-done:
						return
					}
				}
			}()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("⚠️ [IVAS] Disconnected: %v. Reconnecting in 3s...", err)
					close(done)
					_ = conn.Close()
					break
				}

				msgStr := string(message)
				if strings.HasPrefix(msgStr, "42/livesms,") {
					idx := strings.Index(msgStr, "[")
					if idx != -1 {
						var rawData []json.RawMessage
						if err := json.Unmarshal([]byte(msgStr[idx:]), &rawData); err == nil && len(rawData) > 1 {
							var sms IvasSMSPayload
							if err := json.Unmarshal(rawData[1], &sms); err == nil {
								processIvasSMS(bot, sms, botLink)
							}
						}
					}
				}
			}

			time.Sleep(3 * time.Second)
		}
	}()
}

// ── SMS Processing Engine ─────────────────────────────────────────────────────

func processIvasSMS(bot *tgbotapi.BotAPI, sms IvasSMSPayload, botLink string) {
	phoneNum := strings.TrimSpace(sms.Recipient)
	service := strings.TrimSpace(sms.Originator)
	smsText := sms.Message

	if phoneNum == "" {
		return
	}

	if service == "" {
		service = "SMS Service"
	}

	// 🟢 0. Duplicate SMS Filter Check
	if isDuplicateSMS(phoneNum, service, smsText) {
		return
	}

	// 🟢 1. IVAS Number Cut Logic (RAM Cache + SQLite)
	ramUsedNumbersMu.Lock()
	ramUsedNumbers[fmt.Sprintf("%s:%s", phoneNum, service)] = true
	ramUsedNumbersMu.Unlock()

	go func(p, s string) {
		_, _ = sqliteDB.Exec("INSERT OR IGNORE INTO used_numbers (phone_number, service, used_at) VALUES (?, ?, ?)", p, s, time.Now())
	}(phoneNum, service)

	// 🟢 2. Ultra Fast RAM + SQLite Sync Lock Lookup (getUserLockInfo Call)
	targetUserID, countryFile, found := getUserLockInfo(phoneNum)

	if countryFile == "" {
		cleanR := cleanCountryName(sms.Range)
		countryFile = cleanR + "0.txt"
	}

	price := getPriceForCountry(countryFile)
	for _, unpaid := range getUnpaidServices() {
		if strings.EqualFold(service, unpaid) {
			price = 0
			break
		}
	}

	// 🟢 3. Dynamic OTP Sender Calls
	if found && targetUserID > 0 {
		// یوزر اور گروپ دونوں کو میسج بھیجے گا
		sendOtpToUser(bot, targetUserID, phoneNum, service, countryFile, smsText, price)
		sendOtpToGroup(bot, targetUserID, phoneNum, service, countryFile, smsText, botLink, price)

		// 🟢 Balance & Stats Logic
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var u UserData
		_ = usersCollection.FindOne(ctx, bson.M{"id": targetUserID}).Decode(&u)

		now := time.Now().In(time.Local)
		nowStr := now.Format("2006-01-02")

		// 🟢 Ensure Null Maps in MongoDB are Initialized First
		_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID, "api_total_otps": nil}, bson.M{"$set": bson.M{"api_total_otps": bson.M{}}})
		_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID, "api_cycle_otps": nil}, bson.M{"$set": bson.M{"api_cycle_otps": bson.M{}}})
		_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID, "api_cycle_earnings": nil}, bson.M{"$set": bson.M{"api_cycle_earnings": bson.M{}}})
		_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID, "api_earnings": nil}, bson.M{"$set": bson.M{"api_earnings": bson.M{}}})

		incFields := bson.M{
			"balance":                             price,
			"total_earned":                        price,
			"total_otps":                          1,
			"api_earnings.IVAS_WEBSOCKET":         price,
			"api_total_otps.IVAS_WEBSOCKET":       1,     // 🟢 Total OTP Count
			"api_cycle_otps.IVAS_WEBSOCKET":       1,     // 🟢 Cycle OTP Count
			"api_cycle_earnings.IVAS_WEBSOCKET":   price, // 🟢 Cycle Earnings
		}

		setFields := bson.M{
			"last_active": now,
		}

		if u.LastOTPDate.In(time.Local).Format("2006-01-02") != nowStr {
			setFields["today_otps"] = 1
			setFields["last_otp_date"] = now
		} else {
			incFields["today_otps"] = 1
		}

		_, errUpdate := usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID}, bson.M{
			"$inc": incFields,
			"$set": setFields,
		})
		if errUpdate != nil {
			log.Printf("❌ MONGO IVAS BALANCE UPDATE FAILED: %v", errUpdate)
		}

		if u.ReferredBy > 0 && price > 0 {
			refBonus := price * 0.10
			resRef, errRef := usersCollection.UpdateOne(ctx, bson.M{"id": u.ReferredBy}, bson.M{
				"$inc": bson.M{
					"balance":                  refBonus,
					"referral_earnings_earned": refBonus,
				},
			})
			if errRef == nil && resRef.ModifiedCount > 0 {
				go sendRawHTML(bot, u.ReferredBy, txt("{E_GIFT} <b>Referral Profit!</b>\n\nReceived <b>+%.2f %s</b>.", refBonus, CurrencySymbol), smartKB(u.ReferredBy))
			}
		}

		g := getGlobalStats()
		g.TotalOTPsReceived++
		g.TotalEarnings += price
		if g.LastOTPDate.In(time.Local).Format("2006-01-02") != nowStr {
			g.TodayOTPsReceived = 1
			g.LastOTPDate = now
		} else {
			g.TodayOTPsReceived++
		}
		updateGlobalStats(g)

	} else {
		// اگر لاک یوزر نہ ہو تو صرف گروپ میں بھیجیں (System Number)
		sendOtpToGroup(bot, 0, phoneNum, service, countryFile, smsText, botLink, price)
	}
}




// ── IVAS File & Number Management ─────────────────────────────────────────────

func getIvasCountries() []CountryItem {
	files, _ := os.ReadDir(FilesDir)
	var validItems []CountryItem
	globalUsed := getGlobalUsedNumbers()

	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), "0.txt") {
			b, _ := os.ReadFile(filepath.Join(FilesDir, f.Name()))
			lines := strings.Split(string(b), "\n")
			count := 0
			for _, l := range lines {
				n := strings.TrimSpace(l)
				if n != "" && !globalUsed[n] {
					count++
				}
			}
			rawName := strings.TrimSuffix(f.Name(), "0.txt")
			validItems = append(validItems, CountryItem{
				Name:  rawName,
				Count: count,
				File:  f.Name(),
			})
		}
	}
	return validItems
}

func appendIvasNumbersFile(fileName string, newContent string) (int, error) {
	filePath := filepath.Join(FilesDir, fileName)
	existingContent, _ := os.ReadFile(filePath)

	combined := string(existingContent) + "\n" + newContent
	lines := strings.Split(combined, "\n")

	uniqueMap := make(map[string]bool)
	var validNums []string

	for _, l := range lines {
		n := strings.TrimSpace(l)
		if n != "" && !uniqueMap[n] {
			uniqueMap[n] = true
			validNums = append(validNums, n)
		}
	}

	err := os.WriteFile(filePath, []byte(strings.Join(validNums, "\n")), 0644)
	if err != nil {
		return 0, err
	}

	ramNumbersMu.Lock()
	ramNumbers[fileName] = validNums
	ramNumbersMu.Unlock()

	return len(validNums), nil
}

// ── Zip & XML Dynamic Range Extractor ─────────────────────────────────────────

// Zip/XLSX فائل سے تمام کنٹریز اور ان کی رینجز خودکار نکالنے کا فنکشن
func extractAllRangesFromZipXLSX(fileData []byte) (map[string]map[string][]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(fileData), int64(len(fileData)))
	if err != nil {
		return nil, fmt.Errorf("invalid ZIP/XLSX payload: %v", err)
	}

	var sharedStrings []string
	var sheetFiles []*zip.File

	for _, f := range zr.File {
		if strings.HasSuffix(f.Name, "sharedStrings.xml") {
			rc, err := f.Open()
			if err == nil {
				var ss SharedStringsXML
				_ = xml.NewDecoder(rc).Decode(&ss)
				_ = rc.Close()
				for _, item := range ss.SI {
					sharedStrings = append(sharedStrings, item.T)
				}
			}
		} else if strings.Contains(f.Name, "worksheets/sheet") && strings.HasSuffix(f.Name, ".xml") {
			sheetFiles = append(sheetFiles, f)
		}
	}

	// Output Map Structure: BaseCountry -> RawRangeName -> []PhoneNumbers
	countryRangesMap := make(map[string]map[string][]string)

	for _, sf := range sheetFiles {
		rc, err := sf.Open()
		if err != nil {
			continue
		}
		var ws WorksheetXML
		_ = xml.NewDecoder(rc).Decode(&ws)
		_ = rc.Close()

		for _, row := range ws.SheetData.Row {
			var rawRange string
			var rawPhone string

			for _, cell := range row.C {
				colRef := cell.R // e.g. "A4", "B4"
				val := strings.TrimSpace(cell.V)

				if strings.HasPrefix(colRef, "A") {
					if cell.T == "s" {
						idx, errParse := strconv.Atoi(val)
						if errParse == nil && idx < len(sharedStrings) {
							rawRange = sharedStrings[idx]
						}
					} else {
						rawRange = val
					}
				} else if strings.HasPrefix(colRef, "B") {
					rawPhone = val
				}
			}

			// Clean and extract phone number
			if rawPhone != "" {
				// Handle Scientific Notation / Floats like 9.59457E+11 or 923001234567.0
				if strings.Contains(rawPhone, "e+") || strings.Contains(rawPhone, "E+") {
					if fVal, errParse := strconv.ParseFloat(rawPhone, 64); errParse == nil {
						rawPhone = fmt.Sprintf("%.0f", fVal)
					}
				}
				if idx := strings.Index(rawPhone, "."); idx != -1 {
					rawPhone = rawPhone[:idx]
				}

				reDigits := regexp.MustCompile(`\d+`)
				digits := strings.Join(reDigits.FindAllString(rawPhone, -1), "")

				if len(digits) >= 7 && len(digits) <= 15 {
					baseCountry := cleanCountryName(rawRange)
					if baseCountry == "" || baseCountry == "Unknown" {
						// Fallback: try parsing country code from phone number
						numObj, errParse := phonenumbers.Parse("+"+digits, "")
						if errParse == nil {
							region := phonenumbers.GetRegionCodeForNumber(numObj)
							baseCountry = cleanCountryName(region)
						}
					}

					if baseCountry != "" && baseCountry != "Unknown" {
						if countryRangesMap[baseCountry] == nil {
							countryRangesMap[baseCountry] = make(map[string][]string)
						}
						if rawRange == "" {
							rawRange = baseCountry
						}
						countryRangesMap[baseCountry][rawRange] = append(countryRangesMap[baseCountry][rawRange], digits)
					}
				}
			}
		}
	}

	return countryRangesMap, nil
}

// ── Smart File Processor (Auto Splitter & File Saver) ─────────────────────────

func processIvasFileUpload(fileData []byte, originalFileName string, isXLSXorZIP bool) (map[string]int, int, error) {
	createdFilesReport := make(map[string]int)
	totalSaved := 0

	if isXLSXorZIP {
		// ZIP/XLSX سے خودکار تمام کنٹریز اور رینجز حاصل کریں
		countryRangesMap, err := extractAllRangesFromZipXLSX(fileData)
		if err != nil || len(countryRangesMap) == 0 {
			return nil, 0, fmt.Errorf("no valid numbers or ranges found in uploaded archive: %v", err)
		}

		for baseCountry, rangesMap := range countryRangesMap {
			baseFilePrefix := baseCountry + "0"

			// اگر اس کنٹری کی صرف 1 رینج ہے
			if len(rangesMap) == 1 {
				for _, nums := range rangesMap {
					fileName := baseFilePrefix + ".txt"
					filePath := filepath.Join(FilesDir, fileName)

					_ = os.WriteFile(filePath, []byte(strings.Join(nums, "\n")), 0644)

					ramNumbersMu.Lock()
					ramNumbers[fileName] = nums
					ramNumbersMu.Unlock()

					createdFilesReport[fileName] = len(nums)
					totalSaved += len(nums)
				}
			} else {
				// اگر ایک سے زیادہ رینجز ہیں تو -1, -2, -3 الگ الگ فائلیں بنائے گا
				var rawKeys []string
				for k := range rangesMap {
					rawKeys = append(rawKeys, k)
				}
				sort.Strings(rawKeys)

				for idx, rawKey := range rawKeys {
					nums := rangesMap[rawKey]
					if len(nums) == 0 {
						continue
					}

					fileName := fmt.Sprintf("%s-%d.txt", baseFilePrefix, idx+1)
					filePath := filepath.Join(FilesDir, fileName)

					_ = os.WriteFile(filePath, []byte(strings.Join(nums, "\n")), 0644)

					ramNumbersMu.Lock()
					ramNumbers[fileName] = nums
					ramNumbersMu.Unlock()

					createdFilesReport[fileName] = len(nums)
					totalSaved += len(nums)
				}
			}
		}
	} else {
		// اگر سادہ TXT فائل اپلوڈ کی گئی ہے
		cName := cleanCountryName(originalFileName)
		fileName := cName + "0.txt"
		filePath := filepath.Join(FilesDir, fileName)

		lines := strings.Split(string(fileData), "\n")
		var validNums []string
		for _, l := range lines {
			n := strings.TrimSpace(l)
			if n != "" {
				validNums = append(validNums, n)
			}
		}

		if len(validNums) == 0 {
			return nil, 0, fmt.Errorf("no valid phone numbers found in TXT file")
		}

		_ = os.WriteFile(filePath, []byte(strings.Join(validNums, "\n")), 0644)

		ramNumbersMu.Lock()
		ramNumbers[fileName] = validNums
		ramNumbersMu.Unlock()

		createdFilesReport[fileName] = len(validNums)
		totalSaved = len(validNums)
	}

	return createdFilesReport, totalSaved, nil
}

func appendIvasNumbersFromBytes(fileName string, fileData []byte, isXLSXorZIP bool) (int, error) {
	filePath := filepath.Join(FilesDir, fileName)
	existingContent, _ := os.ReadFile(filePath)

	var newNums []string

	if isXLSXorZIP {
		countryRangesMap, err := extractAllRangesFromZipXLSX(fileData)
		if err != nil {
			return 0, err
		}
		for _, rangesMap := range countryRangesMap {
			for _, nums := range rangesMap {
				newNums = append(newNums, nums...)
			}
		}
	} else {
		lines := strings.Split(string(fileData), "\n")
		for _, l := range lines {
			n := strings.TrimSpace(l)
			if n != "" {
				newNums = append(newNums, n)
			}
		}
	}

	combined := string(existingContent) + "\n" + strings.Join(newNums, "\n")
	lines := strings.Split(combined, "\n")

	uniqueMap := make(map[string]bool)
	var finalNums []string

	for _, l := range lines {
		n := strings.TrimSpace(l)
		if n != "" && !uniqueMap[n] {
			uniqueMap[n] = true
			finalNums = append(finalNums, n)
		}
	}

	err := os.WriteFile(filePath, []byte(strings.Join(finalNums, "\n")), 0644)
	if err != nil {
		return 0, err
	}

	ramNumbersMu.Lock()
	ramNumbers[fileName] = finalNums
	ramNumbersMu.Unlock()

	return len(finalNums), nil
}

// ── Document Message Handler ──────────────────────────────────────────────────

func handleDocumentMessage(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	userID := update.Message.From.ID
	chatID := update.Message.Chat.ID
	doc := update.Message.Document

	stateMu.Lock()
	state, hasState := userState[userID]
	stateMu.Unlock()

	if !hasState || doc == nil {
		return
	}

	fileURL, err := bot.GetFileDirectURL(doc.FileID)
	if err != nil {
		go sendRawHTML(bot, chatID, txt("{E_CROSS} <b>File Download Failed!</b>"), smartKB(userID))
		return
	}

	resp, err := http.Get(fileURL)
	if err != nil {
		go sendRawHTML(bot, chatID, txt("{E_CROSS} <b>File Reading Problem!</b>"), smartKB(userID))
		return
	}
	defer resp.Body.Close()

	fileBytes, _ := io.ReadAll(resp.Body)
	fileExt := strings.ToLower(filepath.Ext(doc.FileName))
	isXLSXorZIP := fileExt == ".xlsx" || fileExt == ".zip"

	switch {
	case state == "ivas_await_upload":
		stateMu.Lock()
		delete(userState, userID)
		stateMu.Unlock()

		filesReport, count, err := processIvasFileUpload(fileBytes, doc.FileName, isXLSXorZIP)
		if err != nil || count == 0 {
			sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Processing Error: %v</b>", err), smartKB(userID))
			return
		}

		reportText := txt("{E_TICK} <b>IVAS Archive Processed Successfully!</b>\n\n• <b>Total Numbers Extracted:</b> %d\n• <b>Generated Range Files:</b>\n", count)
		for fName, numCount := range filesReport {
			reportText += fmt.Sprintf("  ├ <code>%s</code> — <b>%d Nums</b>\n", fName, numCount)
		}

		go sendRawHTML(bot, chatID, reportText, smartKB(userID))

	case strings.HasPrefix(state, "ivas_await_append_file:"):
		fName := strings.TrimPrefix(state, "ivas_await_append_file:")
		stateMu.Lock()
		delete(userState, userID)
		stateMu.Unlock()

		total, err := appendIvasNumbersFromBytes(fName, fileBytes, isXLSXorZIP)
		if err != nil {
			sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Failed to append numbers: %v</b>", err), smartKB(userID))
			return
		}
		sendRawHTML(bot, chatID, txt("{E_TICK} File <code>%s</code> Updated! Total Numbers: <b>%d</b>", fName, total), smartKB(userID))
	}
}

// ── IVAS Admin Keyboards & Helpers ───────────────────────────────────────────



func adminIvasEditCountryKB(fileName string) styledInlineKeyboardMarkup {
	cName := strings.TrimSuffix(fileName, "0.txt")
	var rows [][]styledInlineKeyboardButton

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Add More Numbers (Upload File)", CallbackData: "ivas_append_nums:" + fileName, IconCustomEmojiID: ID_ADD, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Set Specific Rate for " + cName, CallbackData: "ivas_set_price:" + fileName, IconCustomEmojiID: ID_WITHDRAW, Style: "primary"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Delete Country File", CallbackData: "ivas_delete_c:" + fileName, IconCustomEmojiID: ID_TRASH, Style: "danger"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to IVAS Manager", CallbackData: "adm_flow:manage_ivas", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}

func getISO2Code(countryName string) string {
	clean := strings.ToUpper(strings.TrimSpace(cleanCountryName(countryName)))

	isoMap := map[string]string{
		"AFGHANISTAN": "AF", "ALBANIA": "AL", "ALGERIA": "DZ", "ARGENTINA": "AR",
		"AUSTRALIA": "AU", "AUSTRIA": "AT", "AZERBAIJAN": "AZ", "BAHRAIN": "BH",
		"BANGLADESH": "BD", "BELARUS": "BY", "BELGIUM": "BE", "BRAZIL": "BR",
		"BULGARIA": "BG", "CAMBODIA": "KH", "CAMEROON": "CM", "CANADA": "CA",
		"CHINA": "CN", "COLOMBIA": "CO", "CROATIA": "HR", "CYPRUS": "CY",
		"CZECH": "CZ", "DENMARK": "DK", "EGYPT": "EG", "ETHIOPIA": "ET",
		"FINLAND": "FI", "FRANCE": "FR", "GEORGIA": "GE", "GERMANY": "DE",
		"GHANA": "GH", "GREECE": "GR", "HONG KONG": "HK", "HUNGARY": "HU",
		"INDIA": "IN", "INDONESIA": "ID", "IRAN": "IR", "IRAQ": "IQ",
		"IRELAND": "IE", "ISRAEL": "IL", "ITALY": "IT", "JAPAN": "JP",
		"JORDAN": "JO", "KAZAKHSTAN": "KZ", "KENYA": "KE", "KUWAIT": "KW",
		"KYRGYZSTAN": "KG", "LAOS": "LA", "LEBANON": "LB", "MALAYSIA": "MY",
		"MEXICO": "MX", "MOLDOVA": "MD", "MONGOLIA": "MN", "MOROCCO": "MA",
		"MYANMAR": "MM", "NEPAL": "NP", "NETHERLANDS": "NL", "NEW ZEALAND": "NZ",
		"NIGERIA": "NG", "NORWAY": "NO", "OMAN": "OM", "PAKISTAN": "PK",
		"PHILIPPINES": "PH", "POLAND": "PL", "PORTUGAL": "PT", "QATAR": "QA",
		"ROMANIA": "RO", "RUSSIA": "RU", "SAUDI": "SA", "SAUDI ARABIA": "SA",
		"SERBIA": "RS", "SINGAPORE": "SG", "SLOVAKIA": "SK", "SOUTH AFRICA": "ZA",
		"SOUTH KOREA": "KR", "SPAIN": "ES", "SRI LANKA": "LK", "SWEDEN": "SE",
		"SWITZERLAND": "CH", "TAIWAN": "TW", "THAILAND": "TH", "TUNISIA": "TN",
		"TURKEY": "TR", "UAE": "AE", "UNITED ARAB EMIRATES": "AE", "UK": "GB",
		"UNITED KINGDOM": "GB", "ENGLAND": "GB", "USA": "US", "UNITED STATES": "US",
		"UZBEKISTAN": "UZ", "VIETNAM": "VN", "YEMEN": "YE", "ZIMBABWE": "ZW",
	}

	if code, ok := isoMap[clean]; ok {
		return code
	}
	if len(clean) == 2 {
		return clean
	}
	return clean
}

// ── Structs for IVAS Hierarchy ───────────────────────────────────────────────

type IvasCountryGroup struct {
	CountryName string
	TotalNums   int
	FilesCount  int
}

type IvasRangeItem struct {
	FileName  string
	RangeName string
	Count     int
	Price     float64
}

// ── Group IVAS Files by Country ──────────────────────────────────────────────

// ── Group IVAS Files by Country (100% RAM Engine) ──────────────────────────────

func getIvasGroupedCountries() []IvasCountryGroup {
	groups := make(map[string]*IvasCountryGroup)
	globalUsed := getGlobalUsedNumbers()

	ramNumbersMu.RLock()
	for name, lines := range ramNumbers {
		if strings.HasSuffix(name, ".txt") && strings.Contains(name, "0") {
			count := 0
			for _, l := range lines {
				n := strings.TrimSpace(l)
				if n != "" && !globalUsed[n] {
					count++
				}
			}

			rawBase := strings.TrimSuffix(name, ".txt")
			cName := cleanCountryName(rawBase)

			if groups[cName] == nil {
				groups[cName] = &IvasCountryGroup{CountryName: cName}
			}
			groups[cName].TotalNums += count
			groups[cName].FilesCount++
		}
	}
	ramNumbersMu.RUnlock()

	var result []IvasCountryGroup
	for _, g := range groups {
		if g.TotalNums > 0 || g.FilesCount > 0 {
			result = append(result, *g)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CountryName < result[j].CountryName
	})
	return result
}

// Get all specific ranges of a country (100% RAM Engine)
func getIvasCountryRanges(targetCountry string) []IvasRangeItem {
	var ranges []IvasRangeItem
	globalUsed := getGlobalUsedNumbers()
	targetClean := strings.ToLower(cleanCountryName(targetCountry))

	ramNumbersMu.RLock()
	for name, lines := range ramNumbers {
		if strings.HasSuffix(name, ".txt") && strings.Contains(name, "0") {
			rawBase := strings.TrimSuffix(name, ".txt")
			cName := strings.ToLower(cleanCountryName(rawBase))

			if cName == targetClean {
				count := 0
				for _, l := range lines {
					n := strings.TrimSpace(l)
					if n != "" && !globalUsed[n] {
						count++
					}
				}

				price := getPriceForCountry(name)

				ranges = append(ranges, IvasRangeItem{
					FileName:  name,
					RangeName: rawBase,
					Count:     count,
					Price:     price,
				})
			}
		}
	}
	ramNumbersMu.RUnlock()

	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].FileName < ranges[j].FileName
	})
	return ranges
}


// Get all specific ranges of a country


// ── IVAS Admin Keyboards (Main Country List) ─────────────────────────────────

func adminIvasManageKB() styledInlineKeyboardMarkup {
	groups := getIvasGroupedCountries()
	var rows [][]styledInlineKeyboardButton

	for _, g := range groups {
		flagID := getFlagID(g.CountryName)
		rows = append(rows, []styledInlineKeyboardButton{
			{
				Text:              fmt.Sprintf("%s (%d Nums - %d Ranges)", g.CountryName, g.TotalNums, g.FilesCount),
				CallbackData:      "ivas_view_c:" + g.CountryName,
				IconCustomEmojiID: flagID,
				Style:             "primary",
			},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Upload / Add IVAS File (XLSX/ZIP/TXT)", CallbackData: "ivas_add_country_trig", IconCustomEmojiID: ID_ADD, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Dashboard", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}

// ── IVAS Country Ranges Sub-Menu ──────────────────────────────────────────────

// ── IVAS Country Ranges Sub-Menu (Simplified: Single Delete Button) ───────────

func adminIvasCountryRangesKB(countryName string) styledInlineKeyboardMarkup {
	ranges := getIvasCountryRanges(countryName)
	var rows [][]styledInlineKeyboardButton

	for _, r := range ranges {
		// صرف ایک ہی سرخ رنگ کا بٹن ہوگا جس پر کلک کرنے سے رینج ڈیلیٹ ہو جائے گی
		rows = append(rows, []styledInlineKeyboardButton{
			{
				Text:              fmt.Sprintf("Delete Range: %s (%d Nums)", r.RangeName, r.Count),
				CallbackData:      "ivas_del_range:" + r.FileName,
				IconCustomEmojiID: ID_TRASH,
				Style:             "danger",
			},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to IVAS Countries", CallbackData: "adm_flow:manage_ivas", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}
