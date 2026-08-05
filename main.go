package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"unicode/utf16"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	_ "github.com/mattn/go-sqlite3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/nyaruka/phonenumbers"
)

// ── Service icon mapping ──────────────────────────────────────────────────────
var serviceIconMap = map[string]string{
	"telegram": "5330237710655306682",
	"whatsapp": "5334998226636390258",
}

// ── Top Users Medals ──────────────────────────────────────────────────────────
var Top10Medals = []string{
	`<tg-emoji emoji-id="5778325047282241647">1️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778507987119247519">2️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778355910917231510">3️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778496953348264834">4️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778429230303941569">5️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778634662884676303">6️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778650382464979758">7️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778572626377052504">8️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778317599808950144">9️⃣</tg-emoji>`,
	`<tg-emoji emoji-id="5778325047282241647">1️⃣</tg-emoji><tg-emoji emoji-id="5778597459877957448">0️⃣</tg-emoji>`,
}
const FallbackMedal = `<tg-emoji emoji-id="5424818078833715060">💠</tg-emoji>`

// Digit emoji IDs for numbers 0-9 (premium emojis) used for ranks >10
var DigitEmojiIDs = []string{
	"5778597459877957448", // 0
	"5778325047282241647", // 1
	"5778507987119247519", // 2
	"5778355910917231510", // 3
	"5778496953348264834", // 4
	"5778429230303941569", // 5
	"5778634662884676303", // 6
	"5778650382464979758", // 7
	"5778572626377052504", // 8
	"5778317599808950144", // 9
}

// getMedalForRank returns the premium emoji medal for a 1‑based rank
func getMedalForRank(rank int) string {
	if rank >= 1 && rank <= len(Top10Medals) {
		return Top10Medals[rank-1]
	}
	rankStr := strconv.Itoa(rank)
	var medal string
	for _, ch := range rankStr {
		digit := int(ch - '0')
		if digit >= 0 && digit <= 9 {
			medal += fmt.Sprintf(`<tg-emoji emoji-id="%s">%c⃣</tg-emoji>`, DigitEmojiIDs[digit], ch)
		}
	}
	return medal
}

// ── Anti-Spam Rate Limiter ────────────────────────────────────────────────────
var (
	userLastAction   = make(map[int64]time.Time)
	userLastActionMu sync.Mutex
)

func isSpamming(userID int64) bool {
	userLastActionMu.Lock()
	defer userLastActionMu.Unlock()
	last, exists := userLastAction[userID]
	now := time.Now()
	// Ignore if clicking faster than 500 milliseconds
	if exists && now.Sub(last) < 500*time.Millisecond {
		userLastAction[userID] = now
		return true
	}
	userLastAction[userID] = now
	return false
}

// ── Storage Core Drivers ──────────────────────────────────────────────────────

var (
	mongoClient     *mongo.Client
	usersCollection *mongo.Collection
	statsCollection *mongo.Collection
	sqliteDB        *sql.DB

	userState       = map[int64]string{}
	withdrawAmounts = map[int64]float64{}
	stateMu         sync.Mutex

	configMu sync.RWMutex

	apiLastSMSHashes    = map[string]map[string]bool{}
	apiInitialHitDone   = map[string]bool{}
	systemOnlineAlerted = false
	trackingMu          sync.Mutex
)

// ── FULL RAM MEMORY CACHE ENGINE (100% IN-MEMORY) ─────────────────────────────

type RAMLockInfo struct {
	UserID      int64
	CountryFile string
	LockedAt    time.Time
}

type RAMUserMsg struct {
	ChatID    int64
	MessageID int
}

var (
	// Numbers RAM Cache: filename -> []string (تمام نمبرز ریم میں)
	ramNumbers      = make(map[string][]string)
	ramNumbersMu    sync.RWMutex

	// Used Numbers RAM Cache: "phone:service" -> bool
	ramUsedNumbers  = make(map[string]bool)
	ramUsedNumbersMu sync.RWMutex

	// User Locks RAM Cache: phone -> RAMLockInfo
	ramUserLocks    = make(map[string]RAMLockInfo)
	ramUserLocksMu  sync.RWMutex

	// User Messages RAM Cache: userID -> RAMUserMsg
	ramUserMsgs     = make(map[int64]RAMUserMsg)
	ramUserMsgsMu   sync.RWMutex

	// Processed OTPs RAM Cache: smsHash -> bool
	ramProcessedOTPs   = make(map[string]bool)
	ramProcessedOTPsMu sync.RWMutex
)
// ── Custom Struct for Premium Emoji Extraction ──────────────────────────────

// ── Raw Telegram Premium Emoji Storage & Extraction ──────────────────────────

// ── Raw Telegram Premium Emoji Storage & Extraction ──────────────────────────

type RawMessageEntity struct {
	Type          string `json:"type"`
	Offset        int    `json:"offset"`
	Length        int    `json:"length"`
	URL           string `json:"url"`
	CustomEmojiID string `json:"custom_emoji_id"` // 🟢 Raw Telegram Custom Emoji ID
}

type RawMessageExtract struct {
	MessageID int                `json:"message_id"`
	Chat      struct{ ID int64 } `json:"chat"`
	Entities  []RawMessageEntity `json:"entities"`
}

type RawUpdateExtract struct {
	UpdateID int                `json:"update_id"`
	Message  *RawMessageExtract `json:"message"`
}

var (
	rawEntitiesMap   = make(map[string][]RawMessageEntity)
	rawEntitiesMapMu sync.RWMutex
)

// 🟢 Country Code & National Number Extractor Helper
func getCountryCodeAndNational(phone string) (string, string) {
	clean := strings.TrimSpace(strings.TrimPrefix(phone, "+"))
	numObj, err := phonenumbers.Parse("+"+clean, "")
	if err == nil && numObj.GetCountryCode() > 0 {
		cc := fmt.Sprintf("%d", numObj.GetCountryCode())
		national := strings.TrimPrefix(clean, cc)
		return cc, national
	}
	// Fallback for custom numbers
	if len(clean) > 3 {
		return clean[:2], clean[2:]
	}
	return "", clean
}


func storeRawEntities(chatID int64, msgID int, entities []RawMessageEntity) {
	if len(entities) == 0 {
		return
	}
	key := fmt.Sprintf("%d:%d", chatID, msgID)
	rawEntitiesMapMu.Lock()
	rawEntitiesMap[key] = entities
	rawEntitiesMapMu.Unlock()
}

func getRawEntities(chatID int64, msgID int) []RawMessageEntity {
	key := fmt.Sprintf("%d:%d", chatID, msgID)
	rawEntitiesMapMu.RLock()
	ents := rawEntitiesMap[key]
	rawEntitiesMapMu.RUnlock()
	return ents
}

// 🟢 Withdraw Breakdown Format Engine
func buildWithdrawalBreakdown(u UserData) string {
	var lines []string
	idx := 1

	// تمام دستیاب پینل کیز (Keys) اکٹھی کریں
	allKeysMap := make(map[string]bool)
	for k := range u.APITotalOTPs {
		allKeysMap[k] = true
	}
	for k := range u.APIEarnings {
		allKeysMap[k] = true
	}

	var keys []string
	for k := range allKeysMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		totalOTPs := u.APITotalOTPs[key]
		
		// 🛑 اگر کسی پینل کی ٹوٹل OTPs 0 ہیں تو وہ شو نہیں ہوگی
		if totalOTPs == 0 {
			continue
		}

		cycleOTPs := u.APICycleOTPs[key]
		cycleEarned := u.APICycleEarnings[key]

		// پینل کا چھوٹا کوڈ/ٹیگ نکالیں (مثلاً np, hs, ivas)
		tag := key
		if strings.HasPrefix(key, "http") || strings.Contains(key, "_") || strings.Contains(key, ".") {
			// URL سے ٹیگ نکالیں
			configMu.RLock()
			for _, base := range API_Bases {
				safe := strings.ReplaceAll(strings.ReplaceAll(base, ".", "_"), "$", "_")
				if safe == key {
					tag = extractApiTag(base)
					break
				}
			}
			configMu.RUnlock()
		} else if key == "IVAS_WEBSOCKET" || strings.ToLower(key) == "ivas" {
			tag = "Ivas"
		}

		// فارمیٹ: 1:Np = (150/500) 300pkr
		line := fmt.Sprintf("%d:%s = (%d/%d) %.2f%s", idx, strings.Title(tag), cycleOTPs, totalOTPs, cycleEarned, CurrencySymbol)
		lines = append(lines, line)
		idx++
	}

	if len(lines) == 0 {
		return "• No active panel OTPs recorded."
	}
	return strings.Join(lines, "\n")
}


// ── Convert Telegram Native Premium Entities to Valid HTML ───────────────────

func convertEntitiesToHTML(text string, ents []RawMessageEntity) string {
	if len(ents) == 0 {
		return text
	}

	utf16Text := utf16.Encode([]rune(text))

	sortedEnts := make([]RawMessageEntity, len(ents))
	copy(sortedEnts, ents)

	sort.Slice(sortedEnts, func(i, j int) bool {
		return sortedEnts[i].Offset > sortedEnts[j].Offset
	})

	for _, e := range sortedEnts {
		if e.Offset < 0 || e.Offset+e.Length > len(utf16Text) {
			continue
		}
		sub := string(utf16.Decode(utf16Text[e.Offset : e.Offset+e.Length]))
		var tag string
		switch e.Type {
		case "bold":
			tag = "<b>" + sub + "</b>"
		case "italic":
			tag = "<i>" + sub + "</i>"
		case "code":
			tag = "<code>" + sub + "</code>"
		case "pre":
			tag = "<pre>" + sub + "</pre>"
		case "text_link":
			tag = fmt.Sprintf(`<a href="%s">%s</a>`, e.URL, sub)
		case "custom_emoji": // 🟢 پریمیم ایموجی کا نیٹیو HTML ٹیگ
			if e.CustomEmojiID != "" {
				tag = fmt.Sprintf(`<tg-emoji emoji-id="%s">%s</tg-emoji>`, e.CustomEmojiID, sub)
			} else {
				tag = sub
			}
		default:
			continue
		}

		tagUtf16 := utf16.Encode([]rune(tag))
		utf16Text = append(utf16Text[:e.Offset], append(tagUtf16, utf16Text[e.Offset+e.Length:]...)...)
	}
	return string(utf16.Decode(utf16Text))
}



// بوٹ بوٹ ہوتے ہی تمام ڈیٹا ڈسک سے اٹھا کر ریم میں لوڈ کر لے گا
func initRAMCache() {
	log.Println("⚡ [RAM ENGINE] Loading all text files, SQLite logs & locks into RAM...")

	// 1. Load All TXT Files into RAM
	files, err := os.ReadDir(FilesDir)
	if err == nil {
		ramNumbersMu.Lock()
		for _, f := range files {
			if !f.IsDir() && strings.HasSuffix(f.Name(), ".txt") {
				b, err := os.ReadFile(filepath.Join(FilesDir, f.Name()))
				if err == nil {
					lines := strings.Split(string(b), "\n")
					var nums []string
					for _, l := range lines {
						n := strings.TrimSpace(l)
						if n != "" {
							nums = append(nums, n)
						}
					}
					ramNumbers[f.Name()] = nums
				}
			}
		}
		ramNumbersMu.Unlock()
	}

	// 2. Load SQLite Used Numbers into RAM
	rows, err := sqliteDB.Query("SELECT phone_number, service FROM used_numbers")
	if err == nil {
		ramUsedNumbersMu.Lock()
		for rows.Next() {
			var p, s string
			rows.Scan(&p, &s)
			ramUsedNumbers[fmt.Sprintf("%s:%s", p, s)] = true
			ramUsedNumbers[p] = true // Global used lookup
		}
		rows.Close()
		ramUsedNumbersMu.Unlock()
	}

	// 3. Load SQLite Processed OTP Hashes into RAM
	pRows, err := sqliteDB.Query("SELECT sms_hash FROM processed_otps")
	if err == nil {
		ramProcessedOTPsMu.Lock()
		for pRows.Next() {
			var h string
			pRows.Scan(&h)
			ramProcessedOTPs[h] = true
		}
		pRows.Close()
		ramProcessedOTPsMu.Unlock()
	}

	log.Println("🚀 [RAM ENGINE] All systems fully loaded into RAM! Ultra 0ms Execution active.")
}


// ── Custom Premium Emoji Mapping ──────────────────────────────────────────────

func ce(id, fallback string) string {
	if !UseCustomEmoji || id == "" {
		return fallback
	}
	return fmt.Sprintf(`<tg-emoji emoji-id="%s">%s</tg-emoji>`, id, fallback)
}

var E_MAP = map[string]string{
	"{E_CROWN}":   ce("6237864166879663987", "👑"),
	"{E_ADMIN}":   ce("6237864166879663987", "👑"),
	"{E_LIVE}":    ce("6235628846855492222", "🔥"),
	"{E_GEAR}":    ce("5424818078833715060", "⚙️"),
	"{E_TICK}":    ce("5895458739703517004", "✅"),
	"{E_CROSS}":   ce("5852812849780362931", "❌"),
	"{E_MOBILE}":  ce("6026092115631543342", "📲"),
	"{E_WARN}":    ce("5213205860498549992", "⚠️"),
	"{E_USERS}":   ce("5453957997418004470", "👥"),
	"{E_GIFT}":    ce("5424818078833715060", "🎁"),
	"{E_NUM}":     ce("5424818078833715060", "🔢"),
	"{E_PLAY}":    ce("5424818078833715060", "▶️"),
	"{E_VIBE}":    ce("5424818078833715060", "📳"),
	"{E_RECEIPT}": ce("5424818078833715060", "🧾"),
	"{E_MONEY}":   ce("6235459831302460476", "💰"),
	"{E_HEART}":   ce("6023924329673135034", "❤️"),
	"{E_MEGA}":    ce("5197304993920616826", "📢"),
	"{E_DOWN}":    ce("5463123191339715467", "⬇️"),
	"{E_DEVIL}":   ce("6246543346597104368", "😈"),
	"{E_SAD}":     ce("6325636659706594831", "😢"),
	"{E_STAR}":    ce("5424818078833715060", "🌟"),
	"{E_BL}":      ce("6235459831302460476", "🌟"),
	"{E_DATA}":    ce("5447410659077661506", "🌟"),
	"{E_SMS}":     ce("5443038326535759644", "🌟"),
	"{E_MINT}":    ce("5778379627726640492", "🌟"),
	"{E_JAZZ}":    ce("5798631291880477829", "🌟"),
	"{E_M}":       ce("5343706594951569383", "🌟"),
	"{E_B}":       ce("5343844407567198128", "🌟"),
	"{E_STATS}":   ce("5231200819986047254", "🌟"),
	"{E_PKG}":     ce("5424818078833715060", "📦"),
	"{E_L1}":      ce("5971972727383264364", "🔸"),
	"{E_L2}":      ce("5971816626796892111", "🔹"),
	"{E_L3}":      ce("5350337715218949216", "🔸"),
	"{E_L4}":      ce("5229229911033530793", "🔹"),
	"{E_OK}":      ce("6023773095284707791", "👌"),
	"{E_DASH}":    ce("6298356878573307709", "➖"),
	"{E_OTP}":     ce("6298717844804733009", "🔑"),
	"{E_CHANNEL}": ce("5282843764451195532", "🔗"),
	"{E_A1}":      ce("5458701355004735404", "🔹"),
	"{E_M1}":      ce("5972124077735807885", "🔹"),
	"{E_LOAD1}":      ce("5971972727383264364", "🔸"),
	"{E_LOAD2}":      ce("5971816626796892111", "🔹"),
	"{E_LOAD3}":      ce("5350337715218949216", "🔸"),
	"{E_LOAD4}":      ce("5229229911033530793", "🔹"),
	
	
	
	"{E_PSTORE}":      ce("5373130604147654226", "🔹"),
	"{E_CHROME}":      ce("5359758030198031389", "🔹"),
	"{E_LOADING}":      ce("5211052376781241550", "🔹"),
	"{E_PTICK1}":      ce("5208540237524911208", "🔹"),
	"{E_FLY}":      ce("5211129162206560202", "🔹"),
	"{E_PTICK2}":      ce("5278628026416909103", "🔹"),
	"{E_DOWN1}":      ce("6203886371363364022", "🔹"),
	"{E_EYE}":      ce("6206366384264320881", "🔹"),
	"{E_BAN}":      ce("6206396878532121864", "🔹"),
	"{E_IQ}":      ce("6203722870548338074", "🔹"),
	"{E_REDWARN}":      ce("6206174450765796040", "🔹"),
//	"{E_WARN}":      ce("6206077285720659346", "🔹"),
	"{E_BELL}":      ce("6206508629286196237", "🔹"),
	"{E_HUNDRED}":      ce("6203738495639360972", "🔹"),
	"{E_TOP}":      ce("6206090539989734881", "🔹"),
	"{E_DOLLAR}":      ce("6206155797722830770", "🔹"),
	"{E_LINK}":      ce("6206497372176913599", "🔹"),
	"{E_TAJ}":      ce("6206096153511990389", "🔹"),
	"{E_LOADER}":      ce("6206118633370818254", "🔹"),
//	"{E_DOWN1}":      ce("6204177183598974956", "🔹"),
	"{E_FREE}":      ce("6203750195130274981", "🔹"),
	"{E_HIGH}":      ce("6206445639295834047", "🔹"),
	"{E_Q}":      ce("6206003549722122915", "🔹"),
	"{E_FIREHEART}":      ce("6206041890895172990", "🔹"),
	"{E_DIAMOND}":      ce("6206220960966646470", "🔹"),
	"{E_RIGHT1}":      ce("6206325217002788818", "🔹"),
	"{E_TGEARN}":      ce("6206471194351245524", "🔹"),
	"{E_ONLINE}":      ce("6269377265348383859", "🔹"),
	"{E_TICK2}":      ce("6269073697059901810", "🔹"),
	"{E_TICK3}":      ce("6269243378332864932", "🔹"),
	"{E_LEFT}":      ce("5332348837405145999", "🔹"),
	"{E_DOWN2}":      ce("6271512469684883558", "🔹"),
	"{E_CROSS2}":      ce("6271611232457855630", "🔹"),
	"{E_NEWPACK}":      ce("5386340832628462681", "🔹"),
	"{E_RIGHT}":      ce("5332684922891025384", "🔹"),
	"{E_CARD}":      ce("6305298855688672996", "🔹"),
	
}

const (
	ID_ADD          = "6033108614724456536"
	ID_USERS          = "6025871229758476400"
	ID_RECEIPT          = "5197269100878907942"
	ID_MANAGE       = "5197269100878907942"
	ID_COPY         = "5472308992514464048"
	ID_LINK         = "5282843764451195532"
	ID_ADMIN        = "5467406098367521267"
	ID_BACK         = "5253997076169115797"
	ID_TRASH        = "5372825386591732174"
	ID_TOGGLE       = "6066348702363031988"
	ID_TICK         = "5895458739703517004"
	ID_CROSS        = "5852812849780362931"
	ID_WITHDRAW     = "5409048419211682843"
	ID_TOPUSERS     = "6235445786759402354"
	ID_SUPPORT      = "5215263059639017128"
	ID_STAR         = "5424818078833715060"
	ID_GLOBE        = "5224450179368767019"
	EmojiStats      = "5231200819986047254"
	EmojiBroadcast  = "6104927893912030655"
	EmojiMobile     = "6026092115631543342"
	EmojiGift       = "5424818078833715060"
	ID_USER         = "6025871229758476400"
	ID_CHNL         = "5271604874419647061"
	ID_USD           = "6206155797722830770"
	ID_PTICK         = "5208540237524911208"
)

var countryFlags = map[string]string{
	"australia":      "5382062173323276195",
	"austria":        "5409096965227031137",
	"azerbaijan":     "5224254431939275524",
	"aland":          "5467839726855666731",
	"albania":        "5442808872202942144",
	"algeria":        "5269400778807720624",
	"american_samoa": "5233381108594260837",
	"anguilla":       "5454237677098384174",
	"angola":         "5221978936791017415",
	"andorra":        "5229127072336589200",
	"antarctica":     "5222477234601732139",
	"antigua":        "5233687283927892876",
	"argentina":      "5262873863036872166",
	"armenia":        "5411455658186778270",
	"aruba":          "5231044964212817289",
	"chagos":         "5454408067040952538",
	"afghanistan":    "5341723801824541640",
	"bahamas":        "5429268893313017179",
	"bangladesh":     "5222131025877936317",
	"barbados":       "5222119223307807441",
	"bahrain":        "5229186179676517309",
	"belarus":        "5382219601054544127",
	"belize":         "5231366665853224722",
	"belgium":        "5411564862025244994",
	"benin":          "5429270924832548299",
	"bermuda":        "5454179609140544421",
	"bulgaria":       "5408875181705799521",
	"bolivia":        "5357272214796255046",
	"bonaire":        "5233482856369504645",
	"bosnia":         "5382033281078275575",
	"botswana":       "5422594827667653377",
	"brazil":         "5202074005346983800",
	"brunei":         "5467415349727083629",
	"burkina":        "5474323070183285988",
	"burundi":        "5357542359649237058",
	"bhutan":         "5420163176098446088",
	"vanuatu":        "5469952197930267656",
	"vatican":        "5443067545198277083",
	"uk":             "5202196682497859879",
	"england":        "5229192892710402006",
	"scotland":       "5226852401822057871",
	"wales":          "5228957348113955582",
	"hungary":        "5409065547541260699",
	"venezuela":      "5228751795274136090",
	"virgin_uk":      "5453884806880315764",
	"virgin_us":      "5231484597065237155",
	"timor":          "5422602597263489621",
	"vietnam":        "5474542319673812606",
	"gabon":          "5408983586680350780",
	"haiti":          "5357490485034236365",
	"guyana":         "5420413662886117194",
	"gambia":         "5420472705801536529",
	"ghana":          "5188676065320511388",
	"guadeloupe":     "5467664243081886165",
	"guatemala":      "5357434603214748263",
	"guinea":         "5408977500711691863",
	"guinea_bissau":  "5429574437286454077",
	"germany":        "5409360418520967565",
	"guernsey":       "5229073617173624053",
	"gibraltar":      "5226496954623603888",
	"honduras":       "5224205572391315540",
	"hong_kong":      "5222395857856374392",
	"grenada":        "5467787680441976258",
	"greenland":      "5221969376193816323",
	"greece":         "5381889099026149370",
	"georgia":        "5440371950708864925",
	"guam":           "5233385291892407085",
	"denmark":        "5381854399985366436",
	"jersey":         "5229188988585130961",
	"djibouti":       "5458586718032634511",
	"dominica":       "5231486851923069595",
	"dominican":      "5427236235615683748",
	"eu":             "5228784522924930237",
	"egypt":          "5226476858471626962",
	"zambia":         "5339279432857171449",
	"sahara":         "5431541386279135269",
	"zimbabwe":       "5357255314099943456",
	"israel":         "5332299462461107995",
	"india":          "5447419223242449630",
	"indonesia":      "5291937150814661333",
	"jordan":         "5460834231468959552",
	"iraq":           "5229059314932528805",
	"iran":           "5271878966347601947",
	"ireland":        "5411194670204069594",
	"iceland":        "5226903563472483349",
	"spain":          "5201957744877248121",
	"italy":          "5449723275628259037",
	"yemen":          "5408878892557542535",
	"cape_verde":     "5233184244473283152",
	"kazakhstan":     "5228718354658769982",
	"cambodia":       "5357374413543062283",
	"cameroon":       "5474681124426884947",
	"canada":         "5382084502858249131",
	"canary":         "5233582259092601024",
	"qatar":          "5228799250367788944",
	"kenya":          "5269725950781699509",
	"cyprus":         "5228997115216149309",
	"kyrgyzstan":     "5427268877367130483",
	"kiribati":       "5231365287168721267",
	"china":          "5431782733376399004",
	"north_korea":    "5341271404329317987",
	"cocos":          "5467863456549978285",
	"colombia":       "5341564321098907092",
	"comoros":        "5422342777511886475",
	"congo_brazza":   "5422479718249151727",
	"congo_kin":      "5269407491841603689",
	"kosovo":         "5442767700646442025",
	"costa_rica":     "5269494559418629149",
	"ivory_coast":    "5411283953984218884",
	"cuba":           "5357035553508308603",
	"kuwait":         "5429154466794317724",
	"curacao":        "5233622988267472134",
	"laos":           "5426971433702012873",
	"latvia":         "5269650286342846979",
	"lesotho":        "5422515422312281871",
	"liberia":        "5422520224085720580",
	"lebanon":        "5427118703835625983",
	"libya":          "5222284437814783192",
	"lithuania":      "5411197345968695511",
	"liechtenstein":  "5226703795953612903",
	"luxembourg":     "5411158944666101440",
	"mauritius":      "5269757084999628216",
	"mauritania":     "5422465115360345921",
	"madagascar":     "5429165814097913547",
	"mayotte":        "5467780911573514412",
	"macau":          "5420505321783179067",
	"malawi":         "5341341330691863561",
	"malaysia":       "5339498171246588311",
	"mali":           "5411259459785730007",
	"maldives":       "5233344051616430964",
	"malta":          "5226954282741283529",
	"morocco":        "5260720207520867861",
	"martinique":     "5470045239806802915",
	"marshall":       "5469803287119150148",
	"mexico":         "5382126374494417411",
	"micronesia":     "5231007765501067757",
	"mozambique":     "5429106139822303027",
	"moldova":        "5442607966517736672",
	"monaco":         "5289959262540277451",
	"mongolia":       "5420481703758019034",
	"montserrat":     "5454038304716504375",
	"myanmar":        "5188162778073935826",
	"namibia":        "5420229786746239476",
	"nauru":          "5233464284930915439",
	"nepal":          "5413521039239955481",
	"niger":          "5339240099546673885",
	"nigeria":        "5411568100430587798",
	"netherlands":    "5411124743841524806",
	"nicaragua":      "5426842228200847679",
	"niue":           "5454251094576218954",
	"new_zealand":    "5269712902671055050",
	"new_caledonia":  "5233223766762338378",
	"norway":         "5382300771641470186",
	"isle_of_man":    "5226538255029121667",
	"norfolk":        "5233192645429312298",
	"christmas":      "5467797026290809748",
	"st_helena":      "5454076894997659542",
	"pitcairn":       "5454181382962036548",
	"turks":          "5454045923988488117",
	"uae":            "5449495646656537594",
	"oman":           "5269663308683688305",
	"cayman":         "5454177075109839093",
	"cook":           "5454192094610473874",
	"pakistan":       "5269660289321679111",
	"palau":          "5222244507503833341",
	"palestine":      "5449405314904369668",
	"panama":         "5269271835299560112",
	"papua":          "5426911591922678926",
	"paraguay":       "5426992955783134297",
	"peru":           "5409100220812239915",
	"poland":         "5291847690940852675",
	"portugal":       "5382075788369605892",
	"puerto_rico":    "5269767070798592290",
	"south_korea":    "5456531898304047227",
	"reunion":        "5420322107068267129",
	"russia":         "5449408995691341691",
	"rwanda":         "5359528550095402400",
	"romania":        "5411159898148840778",
	"salvador":       "5427301849831061043",
	"samoa":          "5233271161726450295",
	"san_marino":     "5228954998766843234",
	"sao_tome":       "5429484951642842713",
	"saudi":          "5202079966761590204",
	"macedonia":      "5442634591020003500",
	"mariana":        "5230969196694748793",
	"seychelles":     "5231446105568329752",
	"st_bart":        "5233616700435348314",
	"st_pierre":      "5231258308123313128",
	"senegal":        "5474274146210817219",
	"st_vincent":     "5467396563540131322",
	"st_kitts":       "5231087492978982103",
	"st_lucia":       "5222280134257551597",
	"serbia":         "5384313376136507326",
	"singapore":      "5292144120993686909",
	"st_martin":      "5461113820955027461",
	"syria":          "5308002793812955097",
	"slovakia":       "5381967160056755878",
	"slovenia":       "5440874620796284751",
	"usa":            "5202021044105257611",
	"solomon":        "5233325407163398971",
	"somalia":        "5474375863921288686",
	"sudan":          "5431435781623260953",
	"suriname":       "5467524222853071645",
	"sierra_leone":   "5411093944631044380",
	"tajikistan":     "5427304285077516492",
	"thailand":       "5341471408071390957",
	"taiwan":         "5222365101595568847",
	"tanzania":       "5269360178481872158",
	"togo":           "5426845148778609404",
	"tokelau":        "5231066898610798438",
	"tonga":          "5467490150877508877",
	"trinidad":       "5420380776321533161",
	"tuvalu":         "5454304115947487098",
	"tunisia":        "5357130455105679545",
	"turkmenistan":   "5422512652058379683",
	"turkey":         "5226948110873278599",
	"uganda":         "5267090670518025063",
	"uzbekistan":     "5449829434334912605",
	"ukraine":        "5447309366568953338",
	"wallis":         "5231000034559934302",
	"uruguay":        "5269256068474616775",
	"faroe":          "5228851794997688064",
	"fiji":           "5454336104863908516",
	"philippines":    "5460873607729129032",
	"finland":        "5382151560182642075",
	"falkland":       "5454214681843481342",
	"france":         "5202132623060640759",
	"french_guiana":  "5233523014313720667",
	"polynesia":      "5467450310760874001",
	"terres_aust":    "5233379493686558144",
	"croatia":        "5262677003210860950",
	"central_af":     "5422628135139031178",
	"chad":           "5411229867461060921",
	"montenegro":     "5440827745523216914",
	"czech":          "5429496861587156146",
	"chile":          "5222427035023977899",
	"switzerland":    "5442703336266543270",
	"sweden":         "5384542551296455687",
	"sri_lanka":      "5341732791191091574",
	"ecuador":        "5359624993586034294",
	"equatorial":     "5447111931217324976",
	"eritrea":        "5420548035232937623",
	"eswatini":       "5422587427438998925",
	"estonia":        "5411174505332615466",
	"ethiopia":       "5269679685393989166",
	"south_georgia":  "5454396500694024535",
	"south_africa":   "5341547124049852736",
	"south_sudan":    "5458535642281552190",
	"jamaica":        "5420144630429667484",
	"japan":          "5456261908069885892",
	"japanese_flag":  "5413869111979556783",
	"default":        "5224450179368767019",
}


// ── Withdrawal Account Model ──────────────────────────────────────────────────
type WithdrawAccount struct {
	Currency    string `bson:"currency" json:"currency"`       // "pkr" or "usd"
	Network     string `bson:"network" json:"network"`         // "trc20", "bep20"
	WalletAddr  string `bson:"wallet_addr" json:"wallet_addr"` // for USD
	BankName    string `bson:"bank_name" json:"bank_name"`     // for PKR
	AccountNo   string `bson:"account_no" json:"account_no"`   // for PKR
	AccountName string `bson:"account_name" json:"account_name"` // for PKR
	IsSet       bool   `bson:"is_set" json:"is_set"`
}

type UserData struct {
	ID                     int64              `bson:"id" json:"id"`
	Username               string             `bson:"username" json:"username"`
	FirstName              string             `bson:"first_name" json:"first_name"`
	Balance                float64            `bson:"balance" json:"balance"`
	TotalSpent             float64            `bson:"total_spent" json:"total_spent"`
	TotalEarned            float64            `bson:"total_earned" json:"total_earned"`
	TotalWithdrawn         float64            `bson:"total_withdrawn" json:"total_withdrawn"`
	TotalOTPs              int                `bson:"total_otps" json:"total_otps"`
	TodayOTPs              int                `bson:"today_otps" json:"today_otps"`
	WeeklyOTPs             int                `bson:"weekly_otps" json:"weekly_otps"`
	LastWeeklyReset        time.Time          `bson:"last_weekly_reset" json:"last_weekly_reset"`
	LastOTPDate            time.Time          `bson:"last_otp_date" json:"last_otp_date"`
	JoinedAt               time.Time          `bson:"joined_at" json:"joined_at"`
	LastActive             time.Time          `bson:"last_active" json:"last_active"`
	ReferredBy             int64              `bson:"referred_by" json:"referred_by"`
	ReferralEarningsEarned float64            `bson:"referral_earnings_earned" json:"referral_earnings_earned"`
	APIEarnings            map[string]float64 `bson:"api_earnings" json:"api_earnings"`
	APITotalOTPs           map[string]int     `bson:"api_total_otps" json:"api_total_otps"`       // 🟢 All-Time Total OTPs Per Panel
	APICycleOTPs           map[string]int     `bson:"api_cycle_otps" json:"api_cycle_otps"`       // 🟢 Current Cycle OTPs Since Last Withdraw
	APICycleEarnings       map[string]float64 `bson:"api_cycle_earnings" json:"api_cycle_earnings"` // 🟢 Current Cycle Earnings Since Last Withdraw
	Currency  string          `bson:"currency" json:"currency"`
	WdAccount WithdrawAccount `bson:"wd_account" json:"wd_account"`
	WithoutCC bool            `bson:"without_cc" json:"without_cc"` // 🟢 یوزر کی وداؤٹ کنٹری 
}


// getWeeklyStartOfWeek calculates the Monday 00:00:00 PKT start of the current week cycle
func getWeeklyStartOfWeek(t time.Time) time.Time {
	weekday := int(t.Weekday())
	if weekday == 0 { // Sunday = 7
		weekday = 7
	}
	daysToSubtract := weekday - 1
	year, month, day := t.Date()
	monday00 := time.Date(year, month, day, 0, 0, 0, 0, t.Location()).AddDate(0, 0, -daysToSubtract)
	return monday00
}


const FixedUSDRate = 280.0

// ── User Currency Preference Resolver ────────────────────────────────────────
func getUserCurrency(u UserData) string {
	// 1. اگر وڈرا اکاؤنٹ سیٹ ہے تو اس کی کرنسی کو ترجیح دیں
	if u.WdAccount.IsSet && u.WdAccount.Currency != "" {
		return u.WdAccount.Currency
	}
	// 2. اگر وڈرا سیٹ نہیں لیکن اسٹارٹ کمانڈ پر سلیکٹ کی تھی
	if u.Currency != "" {
		return u.Currency
	}
	// 3. اگر کچھ بھی سیٹ نہیں تو ڈیفالٹ PKR
	return "pkr"
}

// ── Dynamic Price Formatter ──────────────────────────────────────────────────
func formatPrice(pricePKR float64, currency string) string {
	if currency == "usd" {
		return fmt.Sprintf("%.4f $", pricePKR/FixedUSDRate)
	}
	return fmt.Sprintf("%.2f %s", pricePKR, CurrencySymbol)
}

// ── Dynamic Balance Formatter ────────────────────────────────────────────────
func formatBalance(balancePKR float64, currency string) string {
	if currency == "usd" {
		return fmt.Sprintf("%.4f $", balancePKR/FixedUSDRate)
	}
	return fmt.Sprintf("%.2f %s", balancePKR, CurrencySymbol)
}

// ── Pakistani Mobile Number Strict Validator ──────────────────────────────────
func validatePakistaniNumber(input string) bool {
	// صرف ہندسے الگ کریں
	re := regexp.MustCompile(`\D`)
	cleaned := re.ReplaceAllString(input, "")
	length := len(cleaned)

	switch length {
	case 11:
		// 11 ہندسے: لازمی 03 سے شروع ہو
		return strings.HasPrefix(cleaned, "03")
	case 10:
		// 10 ہندسے: لازمی 3 سے شروع ہو
		return strings.HasPrefix(cleaned, "3")
	case 12:
		// 12 ہندسے: لازمی 923 سے شروع ہو
		return strings.HasPrefix(cleaned, "923")
	default:
		return false
	}
}
// 🟢 Enforce Max 6 Active Locked Numbers Per User & Auto-Unlock Oldest
func enforceUserLockLimit(userID int64, newCount int) {
	now := time.Now()

	type lockEntry struct {
		phone    string
		lockedAt time.Time
	}
	var userActiveLocks []lockEntry

	ramUserLocksMu.Lock()
	for phone, lockInfo := range ramUserLocks {
		if lockInfo.UserID == userID {
			if now.Sub(lockInfo.LockedAt) < 10*time.Minute {
				userActiveLocks = append(userActiveLocks, lockEntry{phone: phone, lockedAt: lockInfo.LockedAt})
			} else {
				delete(ramUserLocks, phone)
			}
		}
	}

    sort.Slice(userActiveLocks, func(i, j int) bool {
    	return userActiveLocks[i].lockedAt.Before(userActiveLocks[j].lockedAt)
    })


	// 🟢 ٹوٹل لاک لمٹ ۱۰ ہے۔ اگر نئے ۵ آ رہے ہیں تو پچھلے ۵ سے زیادہ پرانے خودکار ان لاک ہو جائیں گے
	maxAllowedExisting := 10 - newCount
	if maxAllowedExisting < 0 {
		maxAllowedExisting = 0
	}

	var toUnlock []string
	if len(userActiveLocks) > maxAllowedExisting {
		numToUnlock := len(userActiveLocks) - maxAllowedExisting
		for i := 0; i < numToUnlock; i++ {
			toUnlock = append(toUnlock, userActiveLocks[i].phone)
			delete(ramUserLocks, userActiveLocks[i].phone)
		}
	}
	ramUserLocksMu.Unlock()

	if len(toUnlock) > 0 {
		go func(uid int64, phones []string) {
			for _, p := range phones {
				sqliteDB.Exec("DELETE FROM user_locks WHERE user_id = ? AND phone_number = ?", uid, p)
			}
		}(userID, toUnlock)
	}
}

var (
	userWithoutCC   = make(map[int64]bool)
	userWithoutCCMu sync.RWMutex
)


var emojiReplacer *strings.Replacer

func txt(format string, a ...interface{}) string {
	if emojiReplacer == nil {
		var args []string
		for k, v := range E_MAP {
			args = append(args, k, v)
		}
		emojiReplacer = strings.NewReplacer(args...)
	}
	str := format
	if len(a) > 0 {
		str = fmt.Sprintf(format, a...)
	}
	return emojiReplacer.Replace(str)
}

func formatDisplayName(rawName string) string {
	if strings.Contains(rawName, "-") {
		parts := strings.Split(rawName, "-")
		return fmt.Sprintf("%s - %s", parts[0], strings.Join(parts[1:], "-"))
	}
	return rawName
}


func getGlobalUsedNumbers() map[string]bool {
	used := make(map[string]bool)
	ramUsedNumbersMu.RLock()
	defer ramUsedNumbersMu.RUnlock()
	for k, v := range ramUsedNumbers {
		if strings.Contains(k, ":") {
	
			pNum := strings.Split(k, ":")[0]
			used[pNum] = v
		} else {
			used[k] = v
		}
	}
	return used
}


func getServiceUsedNumbers(service string) map[string]bool {
	used := make(map[string]bool)
	srv := strings.TrimSpace(service)
	ramUsedNumbersMu.RLock()
	defer ramUsedNumbersMu.RUnlock()
	for k, v := range ramUsedNumbers {
		if strings.HasSuffix(k, ":"+srv) {
			pNum := strings.Split(k, ":")[0]
			used[pNum] = v
		}
	}
	return used
}


// ── Models ────────────────────────────────────────────────────────────────────

type GlobalStats struct {
	ID                string    `bson:"id" json:"id"`
	TotalOTPsReceived int       `bson:"total_otps_received" json:"total_otps_received"`
	TodayOTPsReceived int       `bson:"today_otps_received" json:"today_otps_received"`
	LastOTPDate       time.Time `bson:"last_otp_date" json:"last_otp_date"`
	TotalEarnings     float64   `bson:"total_earnings" json:"total_earnings"`
	TotalWithdrawn    float64   `bson:"total_withdrawn" json:"total_withdrawn"`
}

type DataTablesResponse struct {
	AaData [][]interface{} `json:"aaData"`
}

type CountryItem struct {
	Name  string
	Count int
	File  string
}

type AppInfo struct {
	Name      string   `bson:"name" json:"name"`
	IconID    string   `bson:"icon_id" json:"icon_id"`
	Countries []string `bson:"countries" json:"countries"`
}

// ── Structs for Custom Color Interfaces ───────────────────────────────────────

type styledKeyboardButton struct {
	Text              string `json:"text"`
	IconCustomEmojiID string `json:"icon_custom_emoji_id,omitempty"`
	Style             string `json:"style,omitempty"`
}

type styledReplyKeyboardMarkup struct {
	Keyboard       [][]styledKeyboardButton `json:"keyboard"`
	ResizeKeyboard bool                     `json:"resize_keyboard,omitempty"`
}

type copyTextObj struct {
	Text string `json:"text"`
}

type styledInlineKeyboardButton struct {
	Text              string       `json:"text"`
	CallbackData      string       `json:"callback_data,omitempty"`
	URL               string       `json:"url,omitempty"`
	IconCustomEmojiID string       `json:"icon_custom_emoji_id,omitempty"`
	Style             string       `json:"style,omitempty"`
	CopyText          *copyTextObj `json:"copy_text,omitempty"`
}

type styledInlineKeyboardMarkup struct {
	InlineKeyboard [][]styledInlineKeyboardButton `json:"inline_keyboard"`
}

// ── Helper Functions ──────────────────────────────────────────────────────────

func isAdmin(userID int64) bool {
	configMu.RLock()
	defer configMu.RUnlock()
	for _, id := range AdminIDs {
		if id == userID {
			return true
		}
	}
	return false
}

func getFlagID(country string) string {
	c := strings.ToLower(country)
	// نمبرز اور ڈیش دونوں ہٹا دیں (مثلاً venezuela1-1 -> venezuela)
	c = strings.Map(func(r rune) rune {
		if (r >= '0' && r <= '9') || r == '-' {
			return -1
		}
		return r
	}, c)
	c = strings.TrimSpace(c)
	if id, ok := countryFlags[c]; ok {
		return id
	}
	return countryFlags["default"]
}


func buildURL(base, apiType string) string {
	if strings.Contains(base, "?") {
		return base + "&type=" + apiType
	}
	return base + "?type=" + apiType
}

func cleanCountryName(raw string) string {
	r := strings.NewReplacer("-", " ", "_", " ")
	normalized := r.Replace(raw)
	fields := strings.Fields(normalized)
	if len(fields) == 0 {
		return "Unknown"
	}

	// پہلا لفظ (ملک کا نام) الگ کریں
	var isolated strings.Builder
	for _, char := range fields[0] {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') {
			isolated.WriteRune(char)
		} else {
			break
		}
	}
	firstWord := strings.ToLower(isolated.String())
	if firstWord == "" {
		return "Unknown"
	}

	codeMap := map[string]string{
		"pk": "pakistan",
		"in": "india",
		"us": "usa",
		"uk": "uk",
		"bd": "bangladesh",
		"ru": "russia",
		"ae": "uae",
	}
	if mapped, exists := codeMap[firstWord]; exists {
		return strings.Title(mapped)
	}
	return strings.Title(firstWord)
}


func syncConfigToDB() {
	configMu.Lock()
	defer configMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := options.Update().SetUpsert(true)
	_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{
		"$set": bson.M{
			"admin_ids": AdminIDs,
			"api_bases": API_Bases,
		},
	}, opts)
}

func maskAccount(text string) string {
	runes := []rune(text)
	var digitIndexes []int
	for i, r := range runes {
		if r >= '0' && r <= '9' {
			digitIndexes = append(digitIndexes, i)
		}
	}
	if len(digitIndexes) >= 10 {
		maskStart := len(digitIndexes) - 6
		if maskStart < 0 {
			maskStart = len(digitIndexes) / 2
		}
		maskEnd := maskStart + 4
		for i := maskStart; i < maskEnd && i < len(digitIndexes); i++ {
			runes[digitIndexes[i]] = '*'
		}
		return string(runes)
	}
	return text
}

func extractOTP(sms string) string {
	re := regexp.MustCompile(`\b\d{4,8}\b`)
	matches := re.FindAllString(sms, -1)
	if len(matches) > 0 {
		return matches[0]
	}
	return "000000" // Zero OTP Fallback
}

func getServiceIcon(service string) string {
	apps := getActiveApps()
	for _, app := range apps {
		if strings.EqualFold(strings.TrimSpace(app.Name), strings.TrimSpace(service)) {
			if app.IconID != "" {
				return app.IconID
			}
			return DefaultServiceIconID
		}
	}
	if id, ok := serviceIconMap[strings.ToLower(strings.TrimSpace(service))]; ok {
		return id
	}
	return DefaultServiceIconID
}

// ── Unpaid Services Helpers ───────────────────────────────────────────────────

func getUnpaidServices() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res bson.M
	err := statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
	if err != nil {
		return []string{}
	}
	if val, ok := res["unpaid_services"]; ok {
		if arr, ok := val.(bson.A); ok {
			var out []string
			for _, item := range arr {
				if s, ok := item.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
	}
	return []string{}
}

func addUnpaidService(serviceName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := options.Update().SetUpsert(true)
	_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$addToSet": bson.M{"unpaid_services": serviceName}}, opts)
}

func removeUnpaidService(serviceName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := options.Update().SetUpsert(true)
	_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$pull": bson.M{"unpaid_services": serviceName}}, opts)
}

// ── Storage Layers Initialization ─────────────────────────────────────────────

func initStorage() {
	_ = os.MkdirAll(FilesDir, 0755)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientOptions := options.Client().ApplyURI(MongoURI)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal("MongoDB Connection Error: ", err)
	}
	mongoClient = client
	usersCollection = client.Database(DBName).Collection("users")
	statsCollection = client.Database(DBName).Collection("stats")
	adminRecordsCollection = client.Database(DBName).Collection("admin_records")
		

	var res bson.M
	err = statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
	if err == nil {
		configMu.Lock()
		if val, ok := res["admin_ids"]; ok {
			if arr, ok := val.(bson.A); ok {
				var ids []int64
				for _, item := range arr {
					if i, ok := item.(int64); ok {
						ids = append(ids, i)
					}
					if i, ok := item.(int32); ok {
						ids = append(ids, int64(i))
					}
				}
				if len(ids) > 0 {
					AdminIDs = ids
				}
			}
		}
		if val, ok := res["api_bases"]; ok {
			if arr, ok := val.(bson.A); ok {
				var apis []string
				for _, item := range arr {
					if s, ok := item.(string); ok {
						apis = append(apis, s)
					}
				}
				if len(apis) > 0 {
					API_Bases = apis
				}
			}
		}
		configMu.Unlock()
	} else {
		syncConfigToDB()
	}

	// 🟢 WAL Mode اور Busy Timeout کے ساتھ SQLite اوپن کریں
	db, err := sql.Open("sqlite3", "./locks.db?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal("SQLite Core Init Error: ", err)
	}
	sqliteDB = db

	_, _ = sqliteDB.Exec(`CREATE TABLE IF NOT EXISTS user_locks (
		user_id INTEGER NOT NULL,
		phone_number TEXT NOT NULL,
		country_file TEXT NOT NULL,
		locked_at DATETIME,
		PRIMARY KEY(user_id, phone_number)
	);`)

	_, _ = sqliteDB.Exec(`CREATE TABLE IF NOT EXISTS used_numbers (
		phone_number TEXT NOT NULL,
		service TEXT NOT NULL DEFAULT '',
		used_at DATETIME,
		PRIMARY KEY(phone_number, service)
	);`)

	_, _ = sqliteDB.Exec(`CREATE TABLE IF NOT EXISTS user_number_messages (
		user_id INTEGER PRIMARY KEY,
		chat_id INTEGER NOT NULL,
		message_id INTEGER NOT NULL
	);`)
	
	_, _ = sqliteDB.Exec(`CREATE TABLE IF NOT EXISTS processed_otps (
		sms_hash TEXT PRIMARY KEY,
		created_at DATETIME
	);`)

	sqliteDB.Exec("DELETE FROM user_locks")
	sqliteDB.Exec("DELETE FROM user_number_messages")
	log.Println("🧹 Cleared all old locks and message references.")

	initRAMCache()
}


// 2. 64 Characters Truncation Helper
func truncateForCopy(text string, limit int) string {
	runes := []rune(text)
	if len(runes) > limit {
		return string(runes[:limit])
	}
	return text
}


// ✅ نیا 0ms فلی ریم فنکشن (Replace Code):
func isDuplicateSMS(phoneNum, service, smsText string) bool {
	smsHash := fmt.Sprintf("%s:%s:%s", strings.TrimSpace(phoneNum), strings.TrimSpace(service), strings.TrimSpace(smsText))

	ramProcessedOTPsMu.RLock()
	exists := ramProcessedOTPs[smsHash]
	ramProcessedOTPsMu.RUnlock()

	if exists {
		return true // ریم سے سیکنڈ کے ہزارویں حصے میں ڈوپلیکیٹ ریجیکٹ
	}

	// ریم میں سیو کریں
	ramProcessedOTPsMu.Lock()
	ramProcessedOTPs[smsHash] = true
	ramProcessedOTPsMu.Unlock()

	// بیک گراؤنڈ میں ڈسک (SQLite) پر بیک اپ لکھ دیں
	go func(hash string) {
		_, _ = sqliteDB.Exec("INSERT OR IGNORE INTO processed_otps (sms_hash, created_at) VALUES (?, ?)", hash, time.Now())
	}(smsHash)

	return false
}




	

// 🟢 User Active Locked Numbers Fetcher (Oldest Top, Newest Bottom)
func getUserActiveNumbers(userID int64) []string {
	ramUserLocksMu.RLock()
	defer ramUserLocksMu.RUnlock()

	type lockTuple struct {
		phone    string
		lockedAt time.Time
	}
	var activeList []lockTuple
	now := time.Now()

	for p, info := range ramUserLocks {
		if info.UserID == userID && now.Sub(info.LockedAt) < 10*time.Minute {
			activeList = append(activeList, lockTuple{phone: p, lockedAt: info.LockedAt})
		}
	}

	// وقت کے حساب سے ترتیب دیں (پرانے نمبرز اوپر، نئے نیچے)
	sort.Slice(activeList, func(i, j int) bool {
		return activeList[i].lockedAt.Before(activeList[j].lockedAt)
	})

	var nums []string
	for _, item := range activeList {
		nums = append(nums, item.phone)
	}
	return nums
}



func updateUser(u UserData) {
	go func(userData UserData) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Only update safe metadata, NEVER touch balance via $set
		_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": userData.ID}, bson.M{
			"$set": bson.M{
				"username":    userData.Username,
				"first_name":  userData.FirstName,
				"last_active": userData.LastActive,
				"currency":    userData.Currency,
			},
		})
	}(u)
}


func updateGlobalStats(s GlobalStats) {
	// اسے بھی بیک گراؤنڈ میں ڈال دیا گیا ہے
	go func(stats GlobalStats) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "global_records"}, bson.M{"$set": stats})
	}(s)
}


func getPriceForCountry(countryFile string) float64 {
	cleanFile := strings.TrimSuffix(countryFile, ".txt")
	
	// 1. Full Range Key (e.g. "myanmar0-1")
	fullKey := strings.ToLower(strings.TrimSpace(cleanFile))

	// 2. Panel Key (e.g. "myanmar0-1" -> "myanmar0")
	panelKey := fullKey
	if idx := strings.Index(fullKey, "-"); idx != -1 {
		panelKey = fullKey[:idx]
	}

	// 3. Base Country Key (e.g. "myanmar")
	baseCountryKey := strings.ToLower(cleanCountryName(cleanFile))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var res bson.M
	err := statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
	if err == nil {
		if maps, ok := res["country_prices"]; ok {
			if priceMap, ok := maps.(bson.M); ok {
				
				// Priority 1: Specific Range Check (e.g. myanmar0-1)
				if val, ok := priceMap[fullKey]; ok {
					if fVal, ok := getFloatVal(val); ok {
						return fVal
					}
				}
				
				// Priority 2: Specific Panel Check (e.g. myanmar0 or myanmar1)
				if val, ok := priceMap[panelKey]; ok {
					if fVal, ok := getFloatVal(val); ok {
						return fVal
					}
				}
				
				// Priority 3: Base Country Check (e.g. myanmar)
				if val, ok := priceMap[baseCountryKey]; ok {
					if fVal, ok := getFloatVal(val); ok {
						return fVal
					}
				}
			}
		}
		
		// Priority 4: Global Default Price
		if val, ok := res["otp_price"]; ok {
			if fVal, ok := getFloatVal(val); ok {
				return fVal
			}
		}
	}
	return 5.0
}

// Float Data Type Conversion Helper
func getFloatVal(v interface{}) (float64, bool) {
	switch num := v.(type) {
	case float64:
		return num, true
	case int64:
		return float64(num), true
	case int32:
		return float64(num), true
	}
	return 0, false
}


func getGlobalStats() GlobalStats {
	var s GlobalStats
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := statsCollection.FindOne(ctx, bson.M{"id": "global_records"}).Decode(&s)
	if err == mongo.ErrNoDocuments {
		s = GlobalStats{ID: "global_records"}
		_, _ = statsCollection.InsertOne(ctx, s)
	}
	return s
}

func getActiveRanges() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res bson.M
	err := statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
	if err != nil {
		return []string{}
	}
	if val, ok := res["active_ranges"]; ok {
		if arr, ok := val.(bson.A); ok {
			var out []string
			for _, item := range arr {
				if s, ok := item.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
	}
	return []string{}
}

func addActiveRange(rangeName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := options.Update().SetUpsert(true)
	_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$addToSet": bson.M{"active_ranges": rangeName}}, opts)
}

func removeActiveRange(rangeName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := options.Update().SetUpsert(true)
	_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$pull": bson.M{"active_ranges": rangeName}}, opts)
}

// ── Active Services helpers ──────────────────────────────────────────────────

func getActiveApps() []AppInfo {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res bson.M
	err := statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
	if err != nil {
		return []AppInfo{}
	}
	var apps []AppInfo
	if val, ok := res["active_apps"]; ok {
		if arr, ok := val.(bson.A); ok {
			for _, item := range arr {
				if doc, ok := item.(bson.M); ok {
					name, _ := doc["name"].(string)
					iconID, _ := doc["icon_id"].(string)
					countriesRaw, _ := doc["countries"].(bson.A)
					var countries []string
					for _, c := range countriesRaw {
						if s, ok := c.(string); ok {
							countries = append(countries, s)
						}
					}
					if name != "" {
						isNumeric := regexp.MustCompile(`^[0-9]+$`).MatchString(iconID)
						if iconID == "" || !isNumeric {
							iconID = DefaultAppIconID 
						}
						
						apps = append(apps, AppInfo{Name: name, IconID: iconID, Countries: countries})
					}
				}
			}
		}
	}
	return apps
}

type CustomPriceItem struct {
	Key   string
	Price float64
}

func getCustomPricesList() []CustomPriceItem {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res bson.M
	err := statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
	var list []CustomPriceItem
	if err == nil {
		if maps, ok := res["country_prices"]; ok {
			if priceMap, ok := maps.(bson.M); ok {
				for k, v := range priceMap {
					var p float64
					if f, ok := v.(float64); ok { p = f }
					if i, ok := v.(int64); ok { p = float64(i) }
					if i, ok := v.(int32); ok { p = float64(i) }
					if p > 0 {
						list = append(list, CustomPriceItem{Key: k, Price: p})
					}
				}
			}
		}
	}
	return list
}

func deleteCustomPrice(countryKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	targetPath := fmt.Sprintf("country_prices.%s", strings.ToLower(strings.TrimSpace(countryKey)))
	_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$unset": bson.M{targetPath: ""}})
}

func adminManagePricesKB(page int) styledInlineKeyboardMarkup {
	items := getCustomPricesList()
	pageSize := 5 // Max 5 buttons per page
	totalPages := (len(items) + pageSize - 1) / pageSize
	if totalPages == 0 { totalPages = 1 }
	if page < 1 { page = 1 }
	if page > totalPages { page = totalPages }

	start := (page - 1) * pageSize
	end := start + pageSize
	if end > len(items) { end = len(items) }

	var rows [][]styledInlineKeyboardButton
	for i := start; i < end; i++ {
		item := items[i]
		displayName := strings.Title(item.Key)
		rows = append(rows, []styledInlineKeyboardButton{
			{
				Text:              fmt.Sprintf("❌ %s: %.2f %s", displayName, item.Price, CurrencySymbol),
				CallbackData:      "adm_del_price:" + item.Key,
				IconCustomEmojiID: ID_TRASH,
				Style:             "danger",
			},
		})
	}

	// 🟢 5 بٹنز سے زیادہ ہونے پر ہی Next/Back نظر آئیں گے
	if totalPages > 1 {
		prev := page - 1
		if prev < 1 { prev = totalPages }
		next := page + 1
		if next > totalPages { next = 1 }
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("adm_prices_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("adm_prices_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Set New Price Rates", CallbackData: "adm_flow:set_price_trigger", IconCustomEmojiID: ID_ADD, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Dashboard", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}


func saveActiveApps(apps []AppInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := options.Update().SetUpsert(true)
	bsonApps := make(bson.A, 0, len(apps))
	for _, app := range apps {
		countriesArr := make(bson.A, len(app.Countries))
		for i, c := range app.Countries {
			countriesArr[i] = c
		}
		bsonApps = append(bsonApps, bson.M{
			"name":      app.Name,
			"icon_id":   app.IconID,
			"countries": countriesArr,
		})
	}
	_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$set": bson.M{"active_apps": bsonApps}}, opts)
}

func addApp(app AppInfo) {
	apps := getActiveApps()
	apps = append(apps, app)
	saveActiveApps(apps)
}

func deleteAppByName(name string) {
	apps := getActiveApps()
	var newApps []AppInfo
	for _, a := range apps {
		if a.Name != name {
			newApps = append(newApps, a)
		}
	}
	saveActiveApps(newApps)
}

func addCountryToApp(appName, country string) {
	apps := getActiveApps()
	for i, a := range apps {
		if a.Name == appName {
			for _, existing := range a.Countries {
				if existing == country {
					return
				}
			}
			apps[i].Countries = append(apps[i].Countries, country)
			break
		}
	}
	saveActiveApps(apps)
}

func updateApp(oldName, newName, newIcon string) {
	apps := getActiveApps()
	for i, a := range apps {
		if a.Name == oldName {
			apps[i].Name = newName
			if newIcon != "" {
				apps[i].IconID = newIcon
			} else {
				apps[i].IconID = DefaultAppIconID
			}
			break
		}
	}
	saveActiveApps(apps)
}

func removeCountryFromApp(appName, country string) {
	apps := getActiveApps()
	for i, a := range apps {
		if a.Name == appName {
			var newC []string
			for _, c := range a.Countries {
				if c != country {
					newC = append(newC, c)
				}
			}
			apps[i].Countries = newC
			break
		}
	}
	saveActiveApps(apps)
}

// ── Rate Limit Retry Seconds Parser ───────────────────────────────────────────

func parseRetryAfter(desc string) int {
	re := regexp.MustCompile(`(?i)retry after (\d+)`)
	matches := re.FindStringSubmatch(desc)
	if len(matches) > 1 {
		sec, err := strconv.Atoi(matches[1])
		if err == nil {
			return sec
		}
	}
	return 2 // Default wait fallback
}

// ── Raw HTTP Communicator Log Drivers with Safety Validation ─────────────────

func sendRawHTML(bot *tgbotapi.BotAPI, chatID int64, text string, replyMarkup interface{}) {
	params := tgbotapi.Params{
		"chat_id":    strconv.FormatInt(chatID, 10),
		"text":       text,
		"parse_mode": tgbotapi.ModeHTML,
	}

	// 🟢 سیف کی بورڈ چیک
	if markupJSON, ok := getValidReplyMarkupJSON(replyMarkup); ok {
		params["reply_markup"] = markupJSON
	}

	maxAttempts := 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err := bot.MakeRequest("sendMessage", params)
		if err == nil {
			return
		}

		desc := strings.ToLower(resp.Description)

		// 1. Blocked / Deactivated User Handling
		if strings.Contains(desc, "blocked by the user") || strings.Contains(desc, "user is deactivated") {
			if chatID > 0 {
				deleteUserByID(chatID)
			}
			return
		}

		// 2. Flood Control Auto-Retry Handling
		if strings.Contains(desc, "too many requests") || strings.Contains(desc, "retry after") {
			waitTime := parseRetryAfter(resp.Description)
			time.Sleep(time.Duration(waitTime+1) * time.Second)
			continue
		}

		log.Printf("❌ API EXECUTION REJECTION: %v | %s", err, resp.Description)
		break
	}
}

func editRawHTML(bot *tgbotapi.BotAPI, chatID int64, messageID int, text string, replyMarkup interface{}) {
	params := tgbotapi.Params{
		"chat_id":    strconv.FormatInt(chatID, 10),
		"message_id": strconv.Itoa(messageID),
		"text":       text,
		"parse_mode": tgbotapi.ModeHTML,
	}

	// 🟢 سیف کی بورڈ چیک
	if markupJSON, ok := getValidReplyMarkupJSON(replyMarkup); ok {
		params["reply_markup"] = markupJSON
	}

	maxAttempts := 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err := bot.MakeRequest("editMessageText", params)
		if err == nil {
			return
		}

		desc := strings.ToLower(resp.Description)

		if strings.Contains(desc, "blocked by the user") || strings.Contains(desc, "user is deactivated") {
			if chatID > 0 {
				deleteUserByID(chatID)
			}
			return
		}

		if strings.Contains(desc, "too many requests") || strings.Contains(desc, "retry after") {
			waitTime := parseRetryAfter(resp.Description)
			time.Sleep(time.Duration(waitTime+1) * time.Second)
			continue
		}

		log.Printf("❌ API EDIT REJECTION: %v | %s", err, resp.Description)
		break
	}
}


// ── Group OTP Smart Queue Engine ──────────────────────────────────────────────

type GroupQueueMessage struct {
	ChatID      int64
	Text        string
	ReplyMarkup interface{}
}

// 1000 میسجز کا بفر (Buffer) تا کہ رش کے وقت کوئی میسج ضائع نہ ہو
var groupOTPQueue = make(chan GroupQueueMessage, 1000)

// گروپ میں میسج کیو کرنے کا سیف فنکشن
func queueGroupMessage(chatID int64, text string, replyMarkup interface{}) {
	select {
	case groupOTPQueue <- GroupQueueMessage{ChatID: chatID, Text: text, ReplyMarkup: replyMarkup}:
	default:
		log.Println("⚠️ [GROUP QUEUE] Queue full! Emergency fallback handling.")
	}
}

// بیک گراؤنڈ ورکر جو 3.2 سیکنڈز کے سیف وقفے سے ایک ایک میسج بھیجے گا
// بیک گراؤنڈ ورکر جو 1.2 سیکنڈز کے سیف وقفے سے ایک ایک میسج بھیجے گا
func startGroupQueueWorker(bot *tgbotapi.BotAPI) {
	go func() {
		log.Println("🚀 [GROUP QUEUE] OTP Group Rate-Limiter Queue Engine Activated.")
		for msg := range groupOTPQueue {
			sendRawHTML(bot, msg.ChatID, msg.Text, msg.ReplyMarkup)
			// ٹیلیگرام لمٹس کے مطابق 1.2 سیکنڈز کا سیف بریک
			time.Sleep(400 * time.Millisecond)
		}
	}()
}

// ── RAM Garbage Collector Engine ──────────────────────────────────────────────

// ── RAM Garbage Collector Engine (FIXED) ──────────────────────────────────────

func startGarbageCollector() {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		for range ticker.C {
			log.Println("🧹 [RAM GC] Running RAM Memory Cleanup...")

			// 1. Processed OTPs RAM Cache Clean
			ramProcessedOTPsMu.Lock()
			if len(ramProcessedOTPs) > 10000 {
				ramProcessedOTPs = make(map[string]bool)
			}
			ramProcessedOTPsMu.Unlock()

			// 2. Raw Emoji Entities Cache Clean
			rawEntitiesMapMu.Lock()
			if len(rawEntitiesMap) > 5000 {
				rawEntitiesMap = make(map[string][]RawMessageEntity)
			}
			rawEntitiesMapMu.Unlock() // 🟢 FIXED: Lock() is now correctly paired with Unlock()

			// 3. User Tracker Map Clean
			now := time.Now()
			trackerMu.Lock()
			for uid, tracker := range userTrackerMap {
				if now.Sub(tracker.SilentBanUntil) > 1*time.Hour && now.Sub(tracker.HardBannedUntil) > 1*time.Hour {
					delete(userTrackerMap, uid)
				}
			}
			trackerMu.Unlock()

			log.Println("✅ [RAM GC] Memory cleanup finished successfully.")
		}
	}()
}



// ── Blocked / Deactivated User Cleanup Helper ────────────────────────────────

func deleteUserByID(userID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := usersCollection.DeleteOne(ctx, bson.M{"id": userID})
	if err == nil {
		log.Printf("🗑️ Automatically removed blocked/deactivated user ID: %d from database.", userID)
	}
}

// ── Phone Number Masking Helper ───────────────────────────────────────────────

func maskPhoneNumber(phone string) string {
	clean := strings.TrimSpace(strings.TrimPrefix(phone, "+"))
	if len(clean) < 7 {
		return clean
	}

	prefix := ""
	numObj, err := phonenumbers.Parse("+"+clean, "")
	if err == nil {
		prefix = fmt.Sprintf("%d", numObj.GetCountryCode())
	}
	if prefix == "" || len(prefix) >= len(clean)-4 {
		prefix = clean[:2]
	}
	suffix := clean[len(clean)-4:]

	return fmt.Sprintf("%s{E_L1}%s", prefix, suffix)
}



func sendRawHTMLWithID(bot *tgbotapi.BotAPI, chatID int64, text string, replyMarkup interface{}) int {
	var markupJSON []byte
	if replyMarkup != nil {
		markupJSON, _ = json.Marshal(replyMarkup)
	}
	params := tgbotapi.Params{
		"chat_id":    strconv.FormatInt(chatID, 10),
		"text":       text,
		"parse_mode": tgbotapi.ModeHTML,
	}
	if replyMarkup != nil {
		params["reply_markup"] = string(markupJSON)
	}
	resp, err := bot.MakeRequest("sendMessage", params)
	if err != nil {
		log.Printf("❌ API EXECUTION REJECTION: %v | %s", err, resp.Description)
		return 0
	}
	var result struct {
		MessageID int `json:"message_id"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		log.Printf("❌ Failed to parse message_id: %v", err)
		return 0
	}
	return result.MessageID
}

func startBackgroundWorkers(bot *tgbotapi.BotAPI) {
	go startIvasWorker(bot)
	go startGroupQueueWorker(bot) // 🟢 گروپ کیو ورکر سٹارٹ کریں
	go startGarbageCollector()
		


	// 1. Numbers Fetcher Worker
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		client := &http.Client{Timeout: 15 * time.Second}
		for range ticker.C {
			configMu.RLock()
			currentBases := make([]string, len(API_Bases))
			copy(currentBases, API_Bases)
			configMu.RUnlock()

			countryNumbers := make(map[string][]string)

			for idx, apiBase := range currentBases {
				apiUrl := buildURL(apiBase, "numbers")
				resp, err := client.Get(apiUrl)
				if err != nil {
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				var data DataTablesResponse
				if err := json.Unmarshal(body, &data); err != nil {
					continue
				}

				// پینل کے لحاظ سے ڈیٹا اکٹھا کرنے کا مینوئل نقشہ
				// BaseCountry -> RawRangeName -> []PhoneNumbers
				panelRanges := make(map[string]map[string][]string)

				for _, row := range data.AaData {
					if len(row) > 2 {
						fullCountryStr, ok1 := row[0].(string)
						phoneNum, ok2 := row[2].(string)
						if ok1 && ok2 {
							baseName := cleanCountryName(fullCountryStr)
							if panelRanges[baseName] == nil {
								panelRanges[baseName] = make(map[string][]string)
							}
							panelRanges[baseName][fullCountryStr] = append(panelRanges[baseName][fullCountryStr], phoneNum)
						}
					}
				}

				// ہر کنٹری کی الگ الگ رینجز کی فائلز کا نام بنانا
				for baseName, rangesMap := range panelRanges {
					panelNum := idx + 1

					// اگر اس پینل میں اس کنٹری کی صرف 1 رینج ہے
					if len(rangesMap) == 1 {
						filenameKey := fmt.Sprintf("%s%d", baseName, panelNum) // e.g. Venezuela1
						for _, nums := range rangesMap {
							countryNumbers[filenameKey] = append(countryNumbers[filenameKey], nums...)
						}
					} else {
						// اگر ایک سے زیادہ رینجز ہیں تو ترتيب سے 1, 2, 3 نمبرز دیں
						var rawKeys []string
						for k := range rangesMap {
							rawKeys = append(rawKeys, k)
						}
						sort.Strings(rawKeys) // ترتيب برقرار رکھنے کے لیے

						for rangeIdx, rawKey := range rawKeys {
							filenameKey := fmt.Sprintf("%s%d-%d", baseName, panelNum, rangeIdx+1) // e.g. Venezuela1-1, Venezuela1-2
							countryNumbers[filenameKey] = append(countryNumbers[filenameKey], rangesMap[rawKey]...)
						}
					}
				}
			}

			// ڈسک اور ریم (RAM) میں فائلیں سیو اور ریموو کرنے کی مکمل Sync لاجک
			files, err := os.ReadDir(FilesDir)
			if err == nil {
				// موجودہ تمام TXT فائلوں کو چیک کریں
				for _, f := range files {
					if f.IsDir() || !strings.HasSuffix(f.Name(), ".txt") {
						continue
					}

					// IVAS کی فائلیں (جن کے نام میں 0.txt ہوتا ہے) کو محفوظ رکھیں تاکہ وہ ڈیلیٹ نہ ہوں
					// IVAS کی تمام فائلیں (چاہے 0.txt ہوں یا 0-1.txt, 0-2.txt) کو محفوظ رکھیں
                    cleanFName := f.Name()
                    if strings.HasSuffix(cleanFName, "0.txt") || strings.Contains(cleanFName, "0-") {
                        continue
                    }


					suffix := strings.TrimSuffix(f.Name(), ".txt")
					nums, exists := countryNumbers[suffix]
					filePath := filepath.Join(FilesDir, f.Name())

					if exists && len(nums) > 0 {
						// 🟢 اگر اے پی آئی پر ریکارڈ موجود ہے تو ڈسک اور RAM دونوں اپڈیٹ کر دیں
						_ = os.WriteFile(filePath, []byte(strings.Join(nums, "\n")), 0644)
						ramNumbersMu.Lock()
						ramNumbers[f.Name()] = nums
						ramNumbersMu.Unlock()
					} else {
						// 🔴 اگر پینل سے نمبر ختم ہو گئے یا رینج ڈیلیٹ ہو گئی تو ڈسک اور RAM دونوں سے فوراً ریموو کریں
						_ = os.Remove(filePath)
						ramNumbersMu.Lock()
						delete(ramNumbers, f.Name())
						ramNumbersMu.Unlock()
					}
				}

				// اگر اے پی آئی پر کوئی نیا نمبر یا رینج ائی ہے جو پہلے ڈسک پر نہیں تھی
				for name, nums := range countryNumbers {
					fname := name + ".txt"
					filePath := filepath.Join(FilesDir, fname)

					if _, err := os.Stat(filePath); os.IsNotExist(err) && len(nums) > 0 {
						_ = os.WriteFile(filePath, []byte(strings.Join(nums, "\n")), 0644)
						ramNumbersMu.Lock()
						ramNumbers[fname] = nums
						ramNumbersMu.Unlock()
					}
				}
			}
		}
	}()


	// 2. SMS Worker with Permanent SQLite Fingerprint & Dynamic Bot Link (0ms RAM FAST UPGRADE)
	go func() {
		botLink := "https://t.me/" + bot.Self.UserName

		ticker := time.NewTicker(5 * time.Second)
		client := &http.Client{Timeout: 10 * time.Second}
		for range ticker.C {
			configMu.RLock()
			currentBases := make([]string, len(API_Bases))
			copy(currentBases, API_Bases)
			configMu.RUnlock()

			for _, apiBase := range currentBases {
				apiUrl := buildURL(apiBase, "sms")
				resp, err := client.Get(apiUrl)
				if err != nil {
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()

				var data DataTablesResponse
				if err := json.Unmarshal(body, &data); err == nil {
					trackingMu.Lock()
					isInitialHit := !apiInitialHitDone[apiBase]
					trackingMu.Unlock()

					if isInitialHit {
						trackingMu.Lock()
						apiInitialHitDone[apiBase] = true
						if !systemOnlineAlerted {
							systemOnlineAlerted = true
						}
						trackingMu.Unlock()

						for _, row := range data.AaData {
							if len(row) > 4 {
								pNum := safeToString(row[2])
								srv := safeToString(row[3])
								sTxt := safeToString(row[4])
								_ = isDuplicateSMS(pNum, srv, sTxt)
							}
						}
						continue
					}

					unpaidServices := getUnpaidServices()

					for _, row := range data.AaData {
						if len(row) > 4 {
							phoneNum := safeToString(row[2])
							service := html.EscapeString(strings.TrimSpace(safeToString(row[3])))
							smsText := safeToString(row[4])

							if isDuplicateSMS(phoneNum, service, smsText) {
								continue
							}

							ramUsedNumbersMu.Lock()
							ramUsedNumbers[fmt.Sprintf("%s:%s", phoneNum, service)] = true
							ramUsedNumbersMu.Unlock()

							go func(p, s string) {
								_, _ = sqliteDB.Exec("INSERT OR IGNORE INTO used_numbers (phone_number, service, used_at) VALUES (?, ?, ?)", p, s, time.Now())
							}(phoneNum, service)

							// ⚡⚡⚡ 0ms ULTRA FAST RAM LOCK ENGINE ⚡⚡⚡
							var targetUserID int64
							var countryFile string
							foundInRAM := false

							cleanPhone := strings.TrimSpace(strings.TrimPrefix(phoneNum, "+"))
							now := time.Now()

							// 1. RAM Memory First Check (Microsecond Speed Execution)
							ramUserLocksMu.RLock()
							for p, lockInfo := range ramUserLocks {
								pClean := strings.TrimSpace(strings.TrimPrefix(p, "+"))
								if (pClean == cleanPhone || p == phoneNum) && now.Sub(lockInfo.LockedAt) <= 10*time.Minute {
									targetUserID = lockInfo.UserID
									countryFile = lockInfo.CountryFile
									foundInRAM = true
									break
								}
							}
							ramUserLocksMu.RUnlock()

							// 2. SQLite Fallback Backup (Only triggers if 100% missed in RAM)
							if !foundInRAM {
								cutoff := now.Add(-10 * time.Minute)
								_ = sqliteDB.QueryRow(`
									SELECT user_id, country_file FROM user_locks 
									WHERE (phone_number = ? OR phone_number = ? OR phone_number = ?) 
									  AND locked_at >= ? 
									ORDER BY locked_at DESC LIMIT 1`,
									phoneNum, cleanPhone, "+"+cleanPhone, cutoff).Scan(&targetUserID, &countryFile)
							}

							if countryFile == "" {
								cleanR := cleanCountryName(safeToString(row[0]))
								countryFile = cleanR + "0.txt"
							}

							// 🟢 پرائس کیلکولیشن (صرف PKR میں)
							price := getPriceForCountry(countryFile)
							for _, unpaid := range unpaidServices {
								if strings.EqualFold(service, unpaid) {
									price = 0
									break
								}
							}

							// 🟢 If user lock is found (RAM or DB Fallback)
							if targetUserID > 0 {
								// 🟢 Call config.go functions to format and send OTP
								sendOtpToUser(bot, targetUserID, phoneNum, service, countryFile, smsText, price)
								sendOtpToGroup(bot, targetUserID, phoneNum, service, countryFile, smsText, botLink, price)

								ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

								var u UserData
								_ = usersCollection.FindOne(ctx, bson.M{"id": targetUserID}).Decode(&u)

								now := time.Now().In(time.Local)
								nowStr := now.Format("2006-01-02")
								currentWeekStart := getWeeklyStartOfWeek(now)

								// 🟢 API Tag Safe Key Extraction
								safeApiTag := extractApiTag(apiBase)
								if safeApiTag == "" {
									safeApiTag = "default_api"
								}

								// 🟢 Ensure Null Maps in MongoDB are Initialized First
								_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID, "api_total_otps": nil}, bson.M{"$set": bson.M{"api_total_otps": bson.M{}}})
								_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID, "api_cycle_otps": nil}, bson.M{"$set": bson.M{"api_cycle_otps": bson.M{}}})
								_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID, "api_cycle_earnings": nil}, bson.M{"$set": bson.M{"api_cycle_earnings": bson.M{}}})
								_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID, "api_earnings": nil}, bson.M{"$set": bson.M{"api_earnings": bson.M{}}})

								incFields := bson.M{
									"balance":                            price,
									"total_earned":                       price,
									"total_otps":                         1,
									"api_earnings." + safeApiTag:       price,
									"api_total_otps." + safeApiTag:     1,     // 🟢 Total OTP Count
									"api_cycle_otps." + safeApiTag:     1,     // 🟢 Cycle OTP Count
									"api_cycle_earnings." + safeApiTag: price, // 🟢 Cycle Earnings
								}

								setFields := bson.M{
									"last_active": now,
								}

								// Daily OTP Counter
								if u.LastOTPDate.In(time.Local).Format("2006-01-02") != nowStr {
									setFields["today_otps"] = 1
									setFields["last_otp_date"] = now
								} else {
									incFields["today_otps"] = 1
								}

								// Weekly OTP Counter (Resets on Monday Midnight)
								if u.LastWeeklyReset.In(time.Local).Before(currentWeekStart) {
									setFields["weekly_otps"] = 1
									setFields["last_weekly_reset"] = now
								} else {
									incFields["weekly_otps"] = 1
								}

								_, errUpdate := usersCollection.UpdateOne(ctx, bson.M{"id": targetUserID}, bson.M{
									"$inc": incFields,
									"$set": setFields,
								})
								if errUpdate != nil {
									log.Printf("❌ MONGO BALANCE UPDATE FAILED: %v", errUpdate)
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
								cancel()

								// 🟢 Single Clean Global Stats Update Block
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
								// 🟢 System Number OTP -> Send to Group only
								sendOtpToGroup(bot, 0, phoneNum, service, countryFile, smsText, botLink, price)
							}
						}
					}
				}
			}
		}
	}()




	// 3. Lock Expiration Worker (10 Minutes Expiry)
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		for range ticker.C {
			cutoff := time.Now().Add(-10 * time.Minute) // 🟢 10 Minutes Cutoff

			rows, err := sqliteDB.Query("SELECT DISTINCT user_id FROM user_locks WHERE locked_at < ?", cutoff)
			if err != nil {
				continue
			}
			var expiredUsers []int64
			for rows.Next() {
				var uid int64
				rows.Scan(&uid)
				expiredUsers = append(expiredUsers, uid)
			}
			rows.Close()

			sqliteDB.Exec("DELETE FROM user_locks WHERE locked_at < ?", cutoff)

			for _, uid := range expiredUsers {
				var count int
				err := sqliteDB.QueryRow("SELECT COUNT(*) FROM user_locks WHERE user_id = ?", uid).Scan(&count)
				if err == nil && count == 0 {
					var chatID int64
					var msgID int
					err2 := sqliteDB.QueryRow("SELECT chat_id, message_id FROM user_number_messages WHERE user_id = ?", uid).Scan(&chatID, &msgID)
					if err2 == nil {
						go editRawHTML(bot, chatID, msgID, txt("{E_CROSS} <b>Session Expired</b>\nYour number allocation limit (10 mins) has ended. Request new numbers."), nil)
						sqliteDB.Exec("DELETE FROM user_number_messages WHERE user_id = ?", uid)
					}
				}
			}
		}
	}()

}



// ── Keyboards ─────────────────────────────────────────────────────────────────

func smartKB(userID int64) styledReplyKeyboardMarkup {
	keyboard := [][]styledKeyboardButton{
		{
			{Text: "Get Number", IconCustomEmojiID: EmojiMobile, Style: "primary"},
			{Text: "My Account", IconCustomEmojiID: ID_MANAGE, Style: "danger"},
		},
		{
			{Text: "Stats", IconCustomEmojiID: EmojiStats, Style: "success"},
			{Text: "Withdraw", IconCustomEmojiID: ID_WITHDRAW, Style: "primary"},
		},
		{
			{Text: "Top Users", IconCustomEmojiID: ID_TOPUSERS, Style: "danger"},
			{Text: "Rewards", IconCustomEmojiID: EmojiGift, Style: "success"},
		},
		{
			{Text: "Support", IconCustomEmojiID: ID_SUPPORT, Style: "primary"},
			{Text: "Main Channel", IconCustomEmojiID: ID_CHNL, Style: "danger"},
		},
	}
	if isAdmin(userID) {
		keyboard = append(keyboard, []styledKeyboardButton{
			{Text: "Admin Panel", IconCustomEmojiID: ID_ADMIN, Style: "success"},
		})
	}
	return styledReplyKeyboardMarkup{Keyboard: keyboard, ResizeKeyboard: true}
}

// 🟢 Admin Records Manager Functions
func getAdminRecordsTextAndKB(page int) (string, styledInlineKeyboardMarkup) {
	pageSize := 5
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	total, _ := adminRecordsCollection.CountDocuments(ctx, bson.M{})
	totalPages := int((total + int64(pageSize) - 1) / int64(pageSize))
	if totalPages == 0 {
		totalPages = 1
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}

	skip := int64((page - 1) * pageSize)
	opts := options.Find().SetSort(bson.M{"id": -1}).SetSkip(skip).SetLimit(int64(pageSize))

	cursor, err := adminRecordsCollection.Find(ctx, bson.M{}, opts)
	var records []AdminRecord
	if err == nil {
		_ = cursor.All(ctx, &records)
	}

	text := txt("{E_RECEIPT} <b>Admin Accounting & Record Logs</b>\n\n")

	if len(records) == 0 {
		text += "<i>No saved admin records found.</i>\n\n"
	} else {
		for _, r := range records {
			text += fmt.Sprintf("📌 <b>Record #%d</b> [%s]\n<code>%s</code>\n───────────────\n",
				r.ID, r.CreatedAt.Format("2006-01-02 15:04"), html.EscapeString(r.Text))
		}
	}

	var rows [][]styledInlineKeyboardButton

	if totalPages > 1 {
		prev := page - 1
		if prev < 1 {
			prev = totalPages
		}
		next := page + 1
		if next > totalPages {
			next = 1
		}
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("adm_rec_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("adm_rec_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Add New Record", CallbackData: "adm_flow:add_record_trig", IconCustomEmojiID: ID_ADD, Style: "success"},
		{Text: "Delete Record", CallbackData: "adm_flow:del_record_trig", IconCustomEmojiID: ID_TRASH, Style: "danger"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Dashboard", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return text, styledInlineKeyboardMarkup{InlineKeyboard: rows}
}

func getOrCreateUser(from *tgbotapi.User, referrerID int64) UserData {
	var u UserData
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := usersCollection.FindOne(ctx, bson.M{"id": from.ID}).Decode(&u)
	if err == mongo.ErrNoDocuments {
		var finalRef int64 = 0
		if referrerID > 0 && referrerID != from.ID {
			finalRef = referrerID
		}
        u = UserData{
            ID:                     from.ID,
            Username:               from.UserName,
            FirstName:              from.FirstName,
            Balance:                0.0,
            JoinedAt:               time.Now(),
            LastActive:             time.Now(),
            ReferredBy:             finalRef,
            APIEarnings:            make(map[string]float64),
            APITotalOTPs:           make(map[string]int),
            APICycleOTPs:           make(map[string]int),
            APICycleEarnings:       make(map[string]float64),
        }

		_, _ = usersCollection.InsertOne(ctx, u)
	} else {
		// 🚨 BUG FIX: یہاں پورے Struct کا updateUser(u) ختم کر دیا گیا ہے
		// تاکہ نادانستہ طور پر نیا وڈرا اکاؤنٹ اوور رائٹ (Delete) نہ ہو!
		u.LastActive = time.Now()
		if from.UserName != "" && u.Username != from.UserName {
			u.Username = from.UserName
		}
		
		// صرف LastActive اور Username کو منگو ڈی بی میں ایٹامک اپڈیٹ کریں
		go func(uid int64, uname string, act time.Time) {
			c, cld := context.WithTimeout(context.Background(), 5*time.Second)
			defer cld()
			_, _ = usersCollection.UpdateOne(c, bson.M{"id": uid}, bson.M{
				"$set": bson.M{
					"last_active": act,
					"username":    uname,
				},
			})
		}(from.ID, u.Username, u.LastActive)
	}
	userWithoutCCMu.Lock()
	userWithoutCC[u.ID] = u.WithoutCC
	userWithoutCCMu.Unlock()
	return u
}

func adminKB(page int) styledInlineKeyboardMarkup {
	if page < 1 {
		page = 1
	}

	var rows [][]styledInlineKeyboardButton

	if page == 1 {
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Broadcast Omni Message", CallbackData: "adm_flow:broadcast", IconCustomEmojiID: EmojiBroadcast, Style: "primary"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Check User Detail", CallbackData: "adm_flow:check_user_trig", IconCustomEmojiID: ID_USER, Style: "success"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Admin Records / Notes", CallbackData: "adm_flow:admin_records:1", IconCustomEmojiID: ID_RECEIPT, Style: "primary"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Manage Withdraw Settings", CallbackData: "adm_flow:manage_withdraw", IconCustomEmojiID: ID_WITHDRAW, Style: "success"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Manage OTP Price Options", CallbackData: "adm_flow:manage_price", IconCustomEmojiID: ID_WITHDRAW, Style: "success"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Manage Active Ranges", CallbackData: "adm_flow:manage_active_ranges", IconCustomEmojiID: ID_TOGGLE, Style: "danger"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Next Page", CallbackData: "adm_page:2", IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	} else {
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Manage Active Services", CallbackData: "adm_flow:manage_active_apps", IconCustomEmojiID: ID_MANAGE, Style: "primary"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Manage Unpaid Services", CallbackData: "adm_flow:manage_unpaid", IconCustomEmojiID: ID_TRASH, Style: "danger"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Manage Administrators", CallbackData: "adm_flow:manage_admins", IconCustomEmojiID: ID_ADMIN, Style: "success"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Manage API Access Nodes", CallbackData: "adm_flow:manage_apis", IconCustomEmojiID: ID_LINK, Style: "danger"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "View Live System Stats", CallbackData: "adm_flow:live_stats", IconCustomEmojiID: EmojiStats, Style: "primary"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Manage IVAS Numbers (0 Slot)", CallbackData: "adm_flow:manage_ivas", IconCustomEmojiID: ID_GLOBE, Style: "success"},
		})
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back Page", CallbackData: "adm_page:1", IconCustomEmojiID: ID_BACK, Style: "danger"},
		})
	}

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}


func adminManageWithdrawKB() styledInlineKeyboardMarkup {
	return styledInlineKeyboardMarkup{
		InlineKeyboard: [][]styledInlineKeyboardButton{
			{
				{Text: "Set Minimum Withdraw", CallbackData: "adm_flow:set_min_wd_trig", IconCustomEmojiID: ID_MANAGE, Style: "primary"},
			},
			{
				{Text: "Set Withdraw Time", CallbackData: "adm_flow:set_wd_time_trig", IconCustomEmojiID: ID_TOGGLE, Style: "success"},
			},
			{
				{Text: "Back to Dashboard", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
			},
		},
	}
}

// ── Missing Admin Keyboards with Pagination ──────────────────────────────────

func adminManageServicesKB(page int) styledInlineKeyboardMarkup {
	apps := getActiveApps()
	styles := []string{"primary", "success", "danger"}

	pageSize := 5 // Max 5 rows
	totalPages := (len(apps) + pageSize - 1) / pageSize
	if totalPages == 0 { totalPages = 1 }
	if page < 1 { page = 1 }
	if page > totalPages { page = totalPages }

	start := (page - 1) * pageSize
	end := start + pageSize
	if end > len(apps) { end = len(apps) }

	var rows [][]styledInlineKeyboardButton
	for i := start; i < end; i++ {
		app := apps[i]
		style := styles[i%len(styles)]
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: app.Name, CallbackData: "app_adm:" + app.Name, IconCustomEmojiID: app.IconID, Style: style},
		})
	}

	if totalPages > 1 {
		prev := page - 1
		if prev < 1 { prev = totalPages }
		next := page + 1
		if next > totalPages { next = 1 }
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("adm_services_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("adm_services_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Add Active Service", CallbackData: "adm_flow:add_app_trigger", IconCustomEmojiID: ID_ADD, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Dashboard", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})
	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}

func adminActiveRangesKB(page int) styledInlineKeyboardMarkup {
	ranges := getActiveRanges()
	pageSize := 5
	totalPages := (len(ranges) + pageSize - 1) / pageSize
	if totalPages == 0 { totalPages = 1 }
	if page < 1 { page = 1 }
	if page > totalPages { page = totalPages }

	start := (page - 1) * pageSize
	end := start + pageSize
	if end > len(ranges) { end = len(ranges) }

	var rows [][]styledInlineKeyboardButton
	for i := start; i < end; i++ {
		r := ranges[i]
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Delete: " + r, CallbackData: "adm_del_range:" + r, IconCustomEmojiID: ID_TRASH, Style: "danger"},
		})
	}

	if totalPages > 1 {
		prev := page - 1
		if prev < 1 { prev = totalPages }
		next := page + 1
		if next > totalPages { next = 1 }
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("adm_ranges_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("adm_ranges_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Add Active Range", CallbackData: "adm_flow:add_active_range_trigger", IconCustomEmojiID: ID_ADD, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Panel", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})
	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}

func adminManageUnpaidKB(page int) styledInlineKeyboardMarkup {
	services := getUnpaidServices()
	pageSize := 5
	totalPages := (len(services) + pageSize - 1) / pageSize
	if totalPages == 0 { totalPages = 1 }
	if page < 1 { page = 1 }
	if page > totalPages { page = totalPages }

	start := (page - 1) * pageSize
	end := start + pageSize
	if end > len(services) { end = len(services) }

	var rows [][]styledInlineKeyboardButton
	for i := start; i < end; i++ {
		s := services[i]
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "❌ Delete: " + s, CallbackData: "adm_del_unpaid:" + s, IconCustomEmojiID: ID_TRASH, Style: "danger"},
		})
	}

	if totalPages > 1 {
		prev := page - 1
		if prev < 1 { prev = totalPages }
		next := page + 1
		if next > totalPages { next = 1 }
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("adm_unpaid_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("adm_unpaid_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Add Unpaid Service", CallbackData: "adm_flow:add_unpaid_trigger", IconCustomEmojiID: ID_ADD, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Panel", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})
	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}



func adminManageAdminsKB() styledInlineKeyboardMarkup {
	configMu.RLock()
	defer configMu.RUnlock()
	var rows [][]styledInlineKeyboardButton
	for _, id := range AdminIDs {
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: fmt.Sprintf("Remove Admin: %d", id), CallbackData: fmt.Sprintf("adm_del_admin:%d", id), IconCustomEmojiID: ID_TRASH, Style: "danger"},
		})
	}
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Add New Admin Profile", CallbackData: "adm_flow:add_admin_trigger", IconCustomEmojiID: ID_ADD, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Dashboard", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})
	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}

// ── Keyboard JSON Safety Validator ───────────────────────────────────────────

func getValidReplyMarkupJSON(replyMarkup interface{}) (string, bool) {
	if replyMarkup == nil {
		return "", false
	}

	switch v := replyMarkup.(type) {
	case styledInlineKeyboardMarkup:
		if len(v.InlineKeyboard) == 0 {
			return "", false
		}
	case *styledInlineKeyboardMarkup:
		if v == nil || len(v.InlineKeyboard) == 0 {
			return "", false
		}
	}

	b, err := json.Marshal(replyMarkup)
	if err != nil {
		return "", false
	}

	jsonStr := string(b)
	if jsonStr == "null" || jsonStr == `{"inline_keyboard":null}` || jsonStr == `{"inline_keyboard":[]}` {
		return "", false
	}

	return jsonStr, true
}


func adminManageApisKB(page int) styledInlineKeyboardMarkup {
	configMu.RLock()
	defer configMu.RUnlock()

	pageSize := 5 // Max 5 APIs per page
	totalPages := (len(API_Bases) + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}

	start := (page - 1) * pageSize
	end := start + pageSize
	if end > len(API_Bases) {
		end = len(API_Bases)
	}

	var rows [][]styledInlineKeyboardButton
	for idx := start; idx < end; idx++ {
		api := API_Bases[idx]
		tag := extractApiTag(api)

		// URL کو مناسب لمبائی پر ٹرم کرنا
		displayURL := api
		if len(displayURL) > 22 {
			displayURL = displayURL[:19] + "..."
		}

		// بٹن کا نیا فارمیٹ: Delete1 [np] https://mydomain.sit...
		btnText := fmt.Sprintf("Delete%d [%s] %s", idx+1, tag, displayURL)

		rows = append(rows, []styledInlineKeyboardButton{
			{
				Text:              btnText,
				CallbackData:      fmt.Sprintf("adm_del_api:%d", idx),
				IconCustomEmojiID: ID_TRASH,
				Style:             "danger",
			},
		})
	}

	// 🟢 5 APIs سے زیادہ ہونے پر Next/Back کے بٹن خودکار شو ہوں گے
	if totalPages > 1 {
		prev := page - 1
		if prev < 1 {
			prev = totalPages
		}
		next := page + 1
		if next > totalPages {
			next = 1
		}
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("adm_apis_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("adm_apis_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Register New API Link", CallbackData: "adm_flow:add_api_trigger", IconCustomEmojiID: ID_ADD, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Dashboard", CallbackData: "menu:back_to_admin", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}


func getNumberRoutingKB() styledInlineKeyboardMarkup {
	return styledInlineKeyboardMarkup{
		InlineKeyboard: [][]styledInlineKeyboardButton{
			{
				{Text: "Active Ranges", CallbackData: "menu:getnum_active", IconCustomEmojiID: ID_STAR, Style: "success"},
				{Text: "All Ranges", CallbackData: "menu:getnum_all", IconCustomEmojiID: ID_GLOBE, Style: "primary"},
			},
			{
				{Text: "Active Services", CallbackData: "menu:getnum_apps", IconCustomEmojiID: EmojiMobile, Style: "danger"},
			},
		},
	}
}

// ── Withdraw Config Helpers ──────────────────────────────────────────────────

func getMinWithdrawAmount() float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res bson.M
	err := statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
	if err == nil {
		if val, ok := res["min_withdraw"]; ok {
			if fVal, ok := val.(float64); ok {
				return fVal
			}
			if iVal, ok := val.(int64); ok {
				return float64(iVal)
			}
			if iVal, ok := val.(int32); ok {
				return float64(iVal)
			}
		}
	}
	return MinWithdrawAmount // Default Fallback (100.0)
}

func getWithdrawTimeConfig() (int, int, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res bson.M
	err := statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
	if err == nil {
		start := 17
		end := 20
		timeStr := "5PM 8PM"

		if val, ok := res["withdraw_start_hour"]; ok {
			if i, ok := val.(int32); ok { start = int(i) }
			if i, ok := val.(int64); ok { start = int(i) }
		}
		if val, ok := res["withdraw_end_hour"]; ok {
			if i, ok := val.(int32); ok { end = int(i) }
			if i, ok := val.(int64); ok { end = int(i) }
		}
		if val, ok := res["withdraw_time_str"]; ok {
			if s, ok := val.(string); ok { timeStr = s }
		}
		return start, end, timeStr
	}
	return 17, 20, "5PM 8PM"
}

func parseTimeToken(token string) (int, error) {
	token = strings.ToUpper(strings.TrimSpace(token))
	re := regexp.MustCompile(`^(\d{1,2})(?::\d{2})?\s*(AM|PM)?$`)
	matches := re.FindStringSubmatch(token)
	if len(matches) == 0 {
		return 0, fmt.Errorf("invalid time format")
	}

	h, err := strconv.Atoi(matches[1])
	if err != nil || h < 0 || h > 24 {
		return 0, fmt.Errorf("invalid hour")
	}

	ampm := matches[2]
	if ampm == "PM" && h < 12 {
		h += 12
	} else if ampm == "AM" && h == 12 {
		h = 0
	}
	return h, nil
}

func parseWithdrawTimeInput(input string) (int, int, string, error) {
	parts := strings.Fields(strings.TrimSpace(input))
	if len(parts) != 2 {
		return 0, 0, "", fmt.Errorf("enter two time values separated by space (e.g. 5PM 8PM)")
	}

	startH, err1 := parseTimeToken(parts[0])
	endH, err2 := parseTimeToken(parts[1])
	if err1 != nil || err2 != nil {
		return 0, 0, "", fmt.Errorf("invalid format! Example: 5PM 8PM or 17 20")
	}

	displayStr := fmt.Sprintf("%s %s", strings.ToUpper(parts[0]), strings.ToUpper(parts[1]))
	return startH, endH, displayStr, nil
}


// ── Smart Broadcaster Engine (Safe Placeholder Architecture) ─────────────────

// ── Smart Broadcaster Engine (Safe Placeholder Architecture) ─────────────────

func formatBroadcastText(rawText string) string {
	formatted := rawText

	// 🟢 موجودہ HTML اور پریمیم ایموجی ٹیگز کو عارضی طور پر چھپائیں تاکہ ریجیکس ان کو میپ نہ کرے
	var protected []string
	reHTMLTag := regexp.MustCompile(`(?i)<[^>]+>.*?</[^>]+>|<[^>]+/>`)
	formatted = reHTMLTag.ReplaceAllStringFunc(formatted, func(match string) string {
		placeholder := fmt.Sprintf("___PROT_HTML_%d___", len(protected))
		protected = append(protected, match)
		return placeholder
	})

	// 1. Service Line Parsing
	reService := regexp.MustCompile(`(?i)(Service:\s*)([a-zA-Z0-9_\-]+)(\s+(\d{18,20}))?`)
	formatted = reService.ReplaceAllStringFunc(formatted, func(match string) string {
		sub := reService.FindStringSubmatch(match)
		prefix := sub[1]
		sName := sub[2]
		customID := sub[4]

		var iconID string
		if customID != "" {
			iconID = customID
		} else {
			iconID = getServiceIcon(sName)
		}

		return fmt.Sprintf("%s%s ___EMJ_%s_📲___", prefix, sName, iconID)
	})

	// 2. Auto Country Flag Detection
	for country, flagID := range countryFlags {
		if country == "default" {
			continue
		}
		cTitle := strings.Title(strings.ReplaceAll(country, "_", " "))
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(cTitle) + `\b`)
		if re.MatchString(formatted) {
			placeholder := fmt.Sprintf("___EMJ_%s_🏳️___", flagID)
			formatted = re.ReplaceAllStringFunc(formatted, func(m string) string {
				if strings.Contains(formatted, placeholder) {
					return m
				}
				return m + " " + placeholder
			})
		}
	}

	// 3. Standalone Raw Premium Emoji IDs (18-20 digits)
	reRawID := regexp.MustCompile(`\b(\d{18,20})\b`)
	formatted = reRawID.ReplaceAllStringFunc(formatted, func(id string) string {
		return fmt.Sprintf("___EMJ_%s_⭐___", id)
	})

	// 4. Convert {E_TAGS}
	for k, v := range E_MAP {
		formatted = strings.ReplaceAll(formatted, k, v)
	}

	// 5. Final Resolution
	rePlaceholder := regexp.MustCompile(`___EMJ_(\d{18,20})_([^_]+)___`)
	formatted = rePlaceholder.ReplaceAllStringFunc(formatted, func(m string) string {
		sub := rePlaceholder.FindStringSubmatch(m)
		id := sub[1]
		fallback := sub[2]
		if !UseCustomEmoji || id == "" {
			return fallback
		}
		return fmt.Sprintf(`<tg-emoji emoji-id="%s">%s</tg-emoji>`, id, fallback)
	})

	// 🟢 محفوظ کیے گئے نیٹیو پریمیم ایموجی HTML ٹیگز واپس بحال کریں
	for i, p := range protected {
		placeholder := fmt.Sprintf("___PROT_HTML_%d___", i)
		formatted = strings.ReplaceAll(formatted, placeholder, p)
	}

	return formatted
}



func parseBroadcastPayload(botUsername string, input string) (string, interface{}, interface{}) {
	lines := strings.Split(input, "\n")
	var textLines []string
	var btnLines []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(trimmed), "[button]") {
			btnLines = append(btnLines, trimmed)
		} else {
			textLines = append(textLines, line)
		}
	}

	bodyText := formatBroadcastText(strings.Join(textLines, "\n"))

	// اگر براڈکاسٹ میں کوئی بٹن نہ ہو تو nil بھیجو
	if len(btnLines) == 0 {
		return bodyText, nil, nil
	}

	var personalRows [][]styledInlineKeyboardButton
	var groupRows [][]styledInlineKeyboardButton
	botLink := "https://t.me/" + botUsername

	for _, btnLine := range btnLines {
		clean := strings.TrimSpace(btnLine[8:]) // Strip [Button]

		if strings.Contains(clean, "=") {
			parts := strings.SplitN(clean, "=", 2)
			btnName := strings.TrimSpace(parts[0])
			btnURL := strings.TrimSpace(parts[1])

			pBtn := styledInlineKeyboardButton{
				Text:              btnName,
				URL:               btnURL,
				IconCustomEmojiID: ID_LINK,
				Style:             "success",
			}
			gBtn := styledInlineKeyboardButton{
				Text:              btnName,
				URL:               btnURL,
				IconCustomEmojiID: ID_LINK,
				Style:             "success",
			}

			personalRows = append(personalRows, []styledInlineKeyboardButton{pBtn})
			groupRows = append(groupRows, []styledInlineKeyboardButton{gBtn})
		} else {
			btnName := strings.TrimSpace(clean)

			var cbData string
			switch strings.ToLower(btnName) {
			case "get numbers", "get number":
				cbData = "menu:getnum_apps"
			case "withdraw":
				cbData = "user:initiate_wd"
			default:
				cbData = "menu:getnum_apps"
			}

			pBtn := styledInlineKeyboardButton{
				Text:              btnName,
				CallbackData:      cbData,
				IconCustomEmojiID: EmojiMobile,
				Style:             "primary",
			}

			gBtn := styledInlineKeyboardButton{
				Text:              "Bot Link",
				URL:               botLink,
				IconCustomEmojiID: ID_LINK,
				Style:             "primary",
			}

			personalRows = append(personalRows, []styledInlineKeyboardButton{pBtn})
			groupRows = append(groupRows, []styledInlineKeyboardButton{gBtn})
		}
	}

	var pKB interface{} = nil
	var gKB interface{} = nil

	if len(personalRows) > 0 {
		pKB = styledInlineKeyboardMarkup{InlineKeyboard: personalRows}
	}
	if len(groupRows) > 0 {
		gKB = styledInlineKeyboardMarkup{InlineKeyboard: groupRows}
	}

	return bodyText, pKB, gKB
}


func adminEditServiceKB(appName string) styledInlineKeyboardMarkup {
	var rows [][]styledInlineKeyboardButton

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Edit Service Name/Icon", CallbackData: "app_edit:" + appName, IconCustomEmojiID: ID_MANAGE, Style: "primary"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Add Country to Service", CallbackData: "app_addcountry:" + appName, IconCustomEmojiID: ID_ADD, Style: "success"},
	})

	apps := getActiveApps()
	for _, app := range apps {
		if app.Name == appName {
			for _, c := range app.Countries {
				rows = append(rows, []styledInlineKeyboardButton{
					{Text: "❌ Delete: " + c, CallbackData: "app_delcountry:" + appName + ":" + c, IconCustomEmojiID: ID_TRASH, Style: "danger"},
				})
			}
			break
		}
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Delete Service", CallbackData: "app_delete:" + appName, IconCustomEmojiID: ID_TRASH, Style: "danger"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Services", CallbackData: "adm_flow:manage_active_apps", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}

func sendCopyMessage(bot *tgbotapi.BotAPI, targetChatID int64, fromChatID int64, messageID int, replyMarkup interface{}) bool {
	params := tgbotapi.Params{
		"chat_id":      strconv.FormatInt(targetChatID, 10),
		"from_chat_id": strconv.FormatInt(fromChatID, 10),
		"message_id":   strconv.Itoa(messageID),
	}

	if markupJSON, ok := getValidReplyMarkupJSON(replyMarkup); ok {
		params["reply_markup"] = markupJSON
	}

	maxAttempts := 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err := bot.MakeRequest("copyMessage", params)
		if err == nil {
			return true
		}

		desc := strings.ToLower(resp.Description)

		if strings.Contains(desc, "blocked by the user") || strings.Contains(desc, "user is deactivated") {
			if targetChatID > 0 {
				deleteUserByID(targetChatID)
			}
			return false
		}

		if strings.Contains(desc, "too many requests") || strings.Contains(desc, "retry after") {
			waitTime := parseRetryAfter(resp.Description)
			time.Sleep(time.Duration(waitTime+1) * time.Second)
			continue
		}

		log.Printf("❌ BROADCAST COPY REJECTION: %v | %s", err, resp.Description)
		break
	}
	return false
}

func handleBroadcastExecution(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	userID := update.Message.From.ID
	chatID := update.Message.Chat.ID
	adminMsgID := update.Message.MessageID

	stateMu.Lock()
	delete(userState, userID)
	stateMu.Unlock()

	rawInput := update.Message.Text
	if rawInput == "" {
		rawInput = update.Message.Caption
	}

	botUser, _ := bot.GetMe()
	_, personalKB, groupKB := parseBroadcastPayload(botUser.UserName, rawInput)

	sendRawHTML(bot, chatID, txt("{E_MEGA} <b>Broadcasting omni-media payload started in background!</b>"), smartKB(userID))

	go func() {
		// OTP Group میں کاپی بھیجیں
		sendCopyMessage(bot, OtpGroupId, chatID, adminMsgID, groupKB)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()

		cursor, err := usersCollection.Find(ctx, bson.M{})
		count := 0
		if err == nil {
			var allUsers []UserData
			_ = cursor.All(ctx, &allUsers)
			for _, target := range allUsers {
				time.Sleep(35 * time.Millisecond)
				if sendCopyMessage(bot, target.ID, chatID, adminMsgID, personalKB) {
					count++
				}
			}
		}

		sendRawHTML(bot, chatID, txt("{E_TICK} <b>Broadcast successfully finished for %d users!</b>", count), smartKB(userID))
	}()
}


func activeServicesInlineKB(page int) styledInlineKeyboardMarkup {
	apps := getActiveApps()
	styles := []string{"primary", "success", "danger"}
	if len(apps) == 0 {
		return styledInlineKeyboardMarkup{
			InlineKeyboard: [][]styledInlineKeyboardButton{
				{{Text: "No Active Services Configured", CallbackData: "noop", IconCustomEmojiID: ID_CROSS, Style: "danger"}},
				{{Text: "Back to Menu", CallbackData: "menu:change_country", IconCustomEmojiID: ID_BACK, Style: "primary"}},
			},
		}
	}

	// 1. Page size ko 10 kar diya (2 apps per row = 5 rows total)
	pageSize := 10
	totalPages := (len(apps) + pageSize - 1) / pageSize
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > len(apps) {
		end = len(apps)
	}

	var rows [][]styledInlineKeyboardButton

	// 2. Loop 2, 2 karke aage barhega
	for i := start; i < end; i += 2 {
		var row []styledInlineKeyboardButton

		// Pehla App (Left)
		app1 := apps[i]
		style1 := styles[i%len(styles)]
		row = append(row, styledInlineKeyboardButton{
			Text:               app1.Name,
			CallbackData:       "app_select:" + app1.Name,
			IconCustomEmojiID: app1.IconID,
			Style:              style1,
		})

		// Doosra App (Right) - Agar maujood ho
		if i+1 < end {
			app2 := apps[i+1]
			style2 := styles[(i+1)%len(styles)]
			row = append(row, styledInlineKeyboardButton{
				Text:               app2.Name,
				CallbackData:       "app_select:" + app2.Name,
				IconCustomEmojiID: app2.IconID,
				Style:              style2,
			})
		}

		rows = append(rows, row)
	}

	// Pagination Row (Next / Back)
	if totalPages > 1 {
		prev := page - 1
		if prev < 1 {
			prev = totalPages
		}
		next := page + 1
		if next > totalPages {
			next = 1
		}
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("services_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("services_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Menu", CallbackData: "menu:change_country", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}




// ── Handlers ──────────────────────────────────────────────────────────────────

func handleStart(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	u := update.Message.From
	args := update.Message.CommandArguments()
	var referrer int64 = 0

	if args != "" {
		parsed, err := strconv.ParseInt(args, 10, 64)
		if err == nil && parsed != u.ID && parsed > 0 {
			referrer = parsed
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	count, _ := usersCollection.CountDocuments(ctx, bson.M{"id": u.ID})
	isNewUser := count == 0
	userData := getOrCreateUser(u, referrer)

	if isNewUser && userData.ReferredBy > 0 {
		notifyMsg := txt("{E_GIFT} <b>New Referral Joined!</b>\n\nA new user has joined using your referral link. You will automatically receive a 10%% bonus on their OTP earnings!")
		go sendRawHTML(bot, userData.ReferredBy, notifyMsg, nil)
	}

	// 🚨 اگر یوزر کی کرنسی سیٹ نہیں ہے اور وڈرا اکاؤنٹ بھی سیٹ نہیں تو پہلے کرنسی پوچھیں
	if userData.Currency == "" && !userData.WdAccount.IsSet {
		text := txt("{E_CROWN} <b>Welcome to Premium OTP Bot!</b>\n\nPlease select your preferred display currency:")
		kb := styledInlineKeyboardMarkup{
			InlineKeyboard: [][]styledInlineKeyboardButton{
				{
					{Text: "🇵🇰 PKR (Rs)", CallbackData: "set_pref_curr:pkr", IconCustomEmojiID: ID_PTICK, Style: "success"},
					{Text: "🇺🇸 USD ($)", CallbackData: "set_pref_curr:usd", IconCustomEmojiID: ID_USD, Style: "primary"},
				},
			},
		}
		go sendRawHTML(bot, update.Message.Chat.ID, text, kb)
		return
	}

	text := txt("{E_CROWN} <b>Welcome to Premium OTP Bot!</b>\n\n"+
		"{E_MOBILE} High quality virtual numbers available instantly.\n"+
		"{E_STAR} Easy earning and fast withdrawals.")
	go sendRawHTML(bot, update.Message.Chat.ID, text, smartKB(u.ID))
}


func handleGetNumber(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	text := txt("{E_CHANNEL} <b>Select Allocation Route Option:</b>")
	go sendRawHTML(bot, update.Message.Chat.ID, text, getNumberRoutingKB())
}

func handleMyAccount(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	u := getOrCreateUser(update.Message.From, 0)

	displayTodayOTPs := u.TodayOTPs
	if u.LastOTPDate.In(time.Local).Format("2006-01-02") != time.Now().In(time.Local).Format("2006-01-02") {
		displayTodayOTPs = 0
	}

	// یوزر کی ایکٹیو کرنسی معلوم کریں (USD یا PKR)
	curr := getUserCurrency(u)
	balStr := formatBalance(u.Balance, curr)
	earnedStr := formatBalance(u.TotalEarned, curr)
	withdrawnStr := formatBalance(u.TotalWithdrawn, curr)

	accDetailsText := "<code>Not Set</code>"
	btnText := "Set Withdraw Account"

	if u.WdAccount.IsSet {
		btnText = "Change Withdraw Account"
		if u.WdAccount.Currency == "usd" {
			accDetailsText = fmt.Sprintf("\n• <b>Currency:</b> USD ($)\n• <b>Network:</b> %s\n• <b>Address:</b> <code>%s</code>",
				strings.ToUpper(u.WdAccount.Network), u.WdAccount.WalletAddr)
		} else {
			accDetailsText = fmt.Sprintf("\n• <b>Currency:</b> PKR (Rs)\n• <b>Bank/Wallet:</b> %s\n• <b>Account No:</b> <code>%s</code>\n• <b>Name:</b> %s",
				u.WdAccount.BankName, u.WdAccount.AccountNo, u.WdAccount.AccountName)
		}
	}

	text := txt("{E_USERS} <b>Your Account Info</b>\n\n"+
		"{E_USERS} Name: <b>%s</b>\n"+
		"{E_ADMIN} ID: <code>%d</code>\n"+
		"{E_LOAD1} Joined: <b>%s</b>\n\n"+
		"{E_MOBILE} Total OTPs: <b>%d</b>\n"+
		"{E_TICK} Today OTPs: <b>%d</b>\n"+
		"{E_MONEY} Balance: <b>%s</b>\n"+
		"{E_STAR} Total Earned: <b>%s</b>\n"+
		"{E_GEAR} Withdrawn: <b>%s</b>\n\n"+
		"{E_CARD} <b>Withdrawal Account:</b> %s",
		u.FirstName, u.ID, u.JoinedAt.Format("2006-01-02"), u.TotalOTPs, displayTodayOTPs,
		balStr, earnedStr, withdrawnStr, accDetailsText)

	inlineKB := styledInlineKeyboardMarkup{
		InlineKeyboard: [][]styledInlineKeyboardButton{
			{{Text: btnText, CallbackData: "menu:setup_wd_acc", IconCustomEmojiID: ID_MANAGE, Style: "primary"}},
		},
	}

	go sendRawHTML(bot, update.Message.Chat.ID, text, inlineKB)
}



func handleStats(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	totalUsers, _ := usersCollection.CountDocuments(ctx, bson.M{})
	
	dayAgo := time.Now().Add(-24 * time.Hour)
	activeToday, _ := usersCollection.CountDocuments(ctx, bson.M{"last_active": bson.M{"$gte": dayAgo}})
	
	globalStats := getGlobalStats()
	
	displayTodayGlobalOTPs := globalStats.TodayOTPsReceived
	if globalStats.LastOTPDate.In(time.Local).Format("2006-01-02") != time.Now().In(time.Local).Format("2006-01-02") {
		displayTodayGlobalOTPs = 0
	}

	text := txt("{E_STATS} <b>Live Bot Statistics</b>\n\n"+
		"{E_USERS} Total Active Users: <b>%d</b>\n"+
		"{E_TICK} Active Users (24h): <b>%d</b>\n\n"+
		"{E_MOBILE} Total OTPs Delivered: <b>%d</b>\n"+
		"{E_STAR} Today OTPs Delivered: <b>%d</b>\n\n"+
		"{E_MONEY} Total Network Earnings: <b>%.2f %s</b>\n"+
		"{E_GEAR} Total Successfully Withdrawn: <b>%.2f %s</b>",
		totalUsers, activeToday, globalStats.TotalOTPsReceived, displayTodayGlobalOTPs, globalStats.TotalEarnings, CurrencySymbol, globalStats.TotalWithdrawn, CurrencySymbol)
	go sendRawHTML(bot, update.Message.Chat.ID, text, smartKB(update.Message.From.ID))
}

func handleRewards(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	u := getOrCreateUser(update.Message.From, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	totalTeam, _ := usersCollection.CountDocuments(ctx, bson.M{"referred_by": u.ID})
	pipeline := mongo.Pipeline{{{"$match", bson.M{"referred_by": u.ID}}}, {{"$group", bson.M{"_id": nil, "total_otps": bson.M{"$sum": "$total_otps"}, "total_bal": bson.M{"$sum": "$balance"}}}}}
	cursor, _ := usersCollection.Aggregate(ctx, pipeline)
	var agg []bson.M
	var teamOTPs int = 0
	var teamBalances float64 = 0.0
	if err := cursor.All(ctx, &agg); err == nil && len(agg) > 0 {
		if tOTPs, ok := agg[0]["total_otps"].(int32); ok { teamOTPs = int(tOTPs) }
		if tBal, ok := agg[0]["total_bal"].(float64); ok { teamBalances = tBal }
	}
	botUser, _ := bot.GetMe()
	
	refLink := fmt.Sprintf("https://t.me/%s?start=%d", botUser.UserName, u.ID)
	
	text := txt("{E_GIFT} <b>Referral Rewards (10%%)</b>\n\n"+
		"{E_CHANNEL} Link: <code>%s</code>\n\n"+
		"{E_USERS} Team: <b>%d</b>\n"+
		"{E_MOBILE} Team OTPs: <b>%d</b>\n"+
		"{E_MONEY} Team Balance: <b>%.2f %s</b>\n"+
		"{E_GEAR} Your Ref Earnings: <b>%.2f %s</b>",
		refLink, totalTeam, teamOTPs, teamBalances, CurrencySymbol, u.ReferralEarningsEarned, CurrencySymbol)
	go sendRawHTML(bot, update.Message.Chat.ID, text, smartKB(u.ID))
}

func handleWithdraw(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	u := getOrCreateUser(update.Message.From, 0)

	// 1. اگر اکاؤنٹ سیٹ نہیں ہے تو وڈرا کی اجازت نہیں ملے گی
	if !u.WdAccount.IsSet {
		text := txt("{E_WARN} <b>Withdrawal Account Not Set!</b>\n\nYou must configure your Withdrawal Account first before requesting cashout.\n\n👉 Please go to <b>My Account</b> -> Click <b>Set Withdraw Account</b>.")
		go sendRawHTML(bot, update.Message.Chat.ID, text, smartKB(u.ID))
		return
	}

	curr := getUserCurrency(u)
	_, _, timeStr := getWithdrawTimeConfig()
	isTimeOpen := isWithdrawTime()

	var hasMinBalance bool
	var balStr, minLimitStr string

	if curr == "usd" {
		userUSD := u.Balance / FixedUSDRate
		minUSD := 1.0 // Strictly $1 Minimum for USD Users
		hasMinBalance = userUSD >= minUSD
		balStr = fmt.Sprintf("%.2f $", userUSD)
		minLimitStr = fmt.Sprintf("%.2f $", minUSD)
	} else {
		minPKR := getMinWithdrawAmount()
		hasMinBalance = u.Balance >= minPKR
		balStr = fmt.Sprintf("%.2f %s", u.Balance, CurrencySymbol)
		minLimitStr = fmt.Sprintf("%.0f %s", minPKR, CurrencySymbol)
	}

	timeStatus := fmt.Sprintf("{E_CROSS} <b>Withdraw Timing:</b> %s PKT", timeStr)
	if isTimeOpen {
		timeStatus = fmt.Sprintf("{E_PTICK1} <b>Withdraw Window:</b> OPEN (%s PKT)", timeStr)
	}

	text := txt("{E_GEAR} <b>Withdraw System</b>\n\n"+
		"{E_MONEY} Available Balance: <b>%s</b>\n"+
		"{E_WARN} Minimum Limit: <b>%s</b>\n"+
		"{E_STAR} Status: %s\n\n"+
		"Payouts will be sent directly to your configured account.",
		balStr, minLimitStr, timeStatus)

	var inlineKB interface{}

	if hasMinBalance && isTimeOpen {
		inlineKB = styledInlineKeyboardMarkup{
			InlineKeyboard: [][]styledInlineKeyboardButton{
				{{Text: "Request Cashout", CallbackData: "user:initiate_wd_v2", IconCustomEmojiID: ID_MANAGE, Style: "success"}},
			},
		}
	}

	go sendRawHTML(bot, update.Message.Chat.ID, text, inlineKB)
}


func isWithdrawTime() bool {
	start, end, _ := getWithdrawTimeConfig()
	now := time.Now().In(time.Local)
	hour := now.Hour()

	if start <= end {
		return hour >= start && hour < end
	}
	return hour >= start || hour < end
}

func getTopUsersTextAndKB(page int) (string, styledInlineKeyboardMarkup) {
	pageSize := 10
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	totalUsers, _ := usersCollection.CountDocuments(ctx, bson.M{"total_otps": bson.M{"$gt": 0}})
	totalPages := int((totalUsers + int64(pageSize) - 1) / int64(pageSize))
	if totalPages == 0 { totalPages = 1 }
	if page < 1 { page = 1 }
	if page > totalPages { page = totalPages }

	skip := int64((page - 1) * pageSize)
	opts := options.Find().SetSort(bson.M{"total_otps": -1}).SetSkip(skip).SetLimit(int64(pageSize))
	
	cursor, err := usersCollection.Find(ctx, bson.M{"total_otps": bson.M{"$gt": 0}}, opts)
	var userList []UserData
	if err == nil { _ = cursor.All(ctx, &userList) }

	text := txt("{E_LIVE} <b>Top Active Users (Overall)</b>\n\n")
	for i, u := range userList {
		mentionName := u.FirstName
		if u.Username != "" {
			mentionName = fmt.Sprintf("<a href=\"https://t.me/%s\">%s</a>", u.Username, u.FirstName)
		}
		
		globalRank := int(skip) + i
		medal := getMedalForRank(globalRank + 1)
		
		text += fmt.Sprintf("%s %s — <b>%d</b> OTPs\n", medal, mentionName, u.TotalOTPs)
	}

	var rows [][]styledInlineKeyboardButton
	if totalPages > 1 {
		prev := page - 1
		if prev < 1 { prev = totalPages }
		next := page + 1
		if next > totalPages { next = 1 }
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("top_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("top_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	// 🟢 ویکلی ٹاپ یوزر پر جانے کا ان لائن بٹن
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Weekly Top Users", CallbackData: "top_weekly_page:1", IconCustomEmojiID: ID_TOPUSERS, Style: "success"},
	})

	return text, styledInlineKeyboardMarkup{InlineKeyboard: rows}
}

// ── Weekly Top Users Leaderboard (100+ OTPs Required) ───────────────────────
func getWeeklyTopUsersTextAndKB(page int) (string, styledInlineKeyboardMarkup) {
	pageSize := 10
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// صرف 100 یا اس سے زائد OTPs والے یوزرز کا فلٹر
	filter := bson.M{"weekly_otps": bson.M{"$gte": 100}}
	totalUsers, _ := usersCollection.CountDocuments(ctx, filter)

	var rows [][]styledInlineKeyboardButton

	// 🟢 اگر کسی کے 100+ OTPs نہ ہوں تو پیارا سا میسج
	if totalUsers == 0 {
		text := txt("{E_WARN} <b>No Weekly Top Users Yet!</b>\n\n" +
			"To feature on the <b>Weekly Top Users</b> leaderboard, you must complete <b>100+ OTPs</b> in the active week.\n\n" +
			"💡 <i>Weekly rankings automatically reset every Monday night at 12:00 AM PKT. Keep grinding!</i>")
		
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Overall Top Users", CallbackData: "top_page:1", IconCustomEmojiID: ID_TOPUSERS, Style: "primary"},
		})
		return text, styledInlineKeyboardMarkup{InlineKeyboard: rows}
	}

	totalPages := int((totalUsers + int64(pageSize) - 1) / int64(pageSize))
	if totalPages == 0 { totalPages = 1 }
	if page < 1 { page = 1 }
	if page > totalPages { page = totalPages }

	skip := int64((page - 1) * pageSize)
	opts := options.Find().SetSort(bson.M{"weekly_otps": -1}).SetSkip(skip).SetLimit(int64(pageSize))

	cursor, err := usersCollection.Find(ctx, filter, opts)
	var userList []UserData
	if err == nil { _ = cursor.All(ctx, &userList) }

	text := txt("{E_LIVE} <b>Weekly Top Users</b>\n" +
		"<i>{E_PTICK1} Resets every Monday night at 12:00 AM</i>\n\n")

	for i, u := range userList {
		mentionName := u.FirstName
		if u.Username != "" {
			mentionName = fmt.Sprintf("<a href=\"https://t.me/%s\">%s</a>", u.Username, u.FirstName)
		}
		
		globalRank := int(skip) + i
		medal := getMedalForRank(globalRank + 1)
		
		text += fmt.Sprintf("%s %s — <b>%d</b> OTPs\n", medal, mentionName, u.WeeklyOTPs)
	}

	if totalPages > 1 {
		prev := page - 1
		if prev < 1 { prev = totalPages }
		next := page + 1
		if next > totalPages { next = 1 }
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("top_weekly_page:%d", prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("top_weekly_page:%d", next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	// 👑 واپس مین (Overall) ٹاپ یوزرز پر جانے کا بٹن
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Overall Top Users", CallbackData: "top_page:1", IconCustomEmojiID: ID_TOPUSERS, Style: "primary"},
	})

	return text, styledInlineKeyboardMarkup{InlineKeyboard: rows}
}


func handleTopUsers(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	text, kb := getTopUsersTextAndKB(1)
	go sendRawHTML(bot, update.Message.Chat.ID, text, kb)
}

func handleSupport(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	text := txt("{E_CHANNEL} <b>Customer Support Channel</b>\n\n"+
		"If you face any problem regarding numbers or payments, feel free to contact us. We are available 24/7.")
	inlineKB := styledInlineKeyboardMarkup{
		InlineKeyboard: [][]styledInlineKeyboardButton{
			{{Text: "Developer", URL: DeveloperLink, IconCustomEmojiID: ID_ADMIN, Style: "primary"}},
			{{Text: "Admin Support", URL: AdminSupportLink, IconCustomEmojiID: ID_SUPPORT, Style: "success"}},
		},
	}
	go sendRawHTML(bot, update.Message.Chat.ID, text, inlineKB)

}

func handleMainChannel(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	text := txt("{E_MEGA} <b>Our Official Channels & Groups</b>\n\n"+
		"Join our channels for the latest updates, payment proofs, and connecting with other users!")
	inlineKB := styledInlineKeyboardMarkup{
		InlineKeyboard: [][]styledInlineKeyboardButton{
			{{Text: "Main Channel", URL: MainChannelLink, IconCustomEmojiID: ID_CHNL, Style: "primary"}},
			{{Text: "OTP Group", URL: OtpGroupInviteLink, IconCustomEmojiID: ID_USERS, Style: "success"}},
			{{Text: "Withdraw Proofs", URL: WithdrawProofsLink, IconCustomEmojiID: ID_RECEIPT, Style: "danger"}},
			{{Text: "Backup Channel", URL: BckpChnl, IconCustomEmojiID: ID_RECEIPT, Style: "primary"}},
		},
	}
	go sendRawHTML(bot, update.Message.Chat.ID, text, inlineKB)

}

func handleSupportPanel(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	text := txt("{E_ADMIN} <b>Premium Control Center Admin Panel</b>\n\nWelcome back administrator! Manage configurations below.")
	go sendRawHTML(bot, update.Message.Chat.ID, text, adminKB(1))}

// ── Callback Infrastructure ───────────────────────────────────────────────────
func handleCallback(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	cb := update.CallbackQuery
	chatID := cb.Message.Chat.ID
	msgID := cb.Message.MessageID
	data := cb.Data
	go bot.Request(tgbotapi.NewCallback(cb.ID, ""))

	if strings.HasPrefix(data, "top_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "top_page:"))
		text, kb := getTopUsersTextAndKB(page)
		go editRawHTML(bot, chatID, msgID, text, kb)
		return
	}
	
		// 🟢 Admin Panel Page Switcher
	if strings.HasPrefix(data, "adm_page:") {
		if !isAdmin(cb.From.ID) {
			return
		}
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_page:"))
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Premium Control Center Admin Panel</b>"), adminKB(page))
		return
	}
	
	// 🟢 Permanent User Preference Toggle Handler
	if strings.HasPrefix(data, "tgcc_toggle:") {
		fileName := strings.TrimPrefix(data, "tgcc_toggle:")
		userID := cb.From.ID

		// 1. RAM اور ڈیٹا بیس میں یوزر کی پسند فلپ (Toggle) کریں
		userWithoutCCMu.Lock()
		newVal := !userWithoutCC[userID]
		userWithoutCC[userID] = newVal
		userWithoutCCMu.Unlock()

		// 2. منگو ڈی بی میں بیک گراؤنڈ سیو کریں
		go func(uid int64, val bool) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": uid}, bson.M{"$set": bson.M{"without_cc": val}})
		}(userID, newVal)

		// 3. اسکرین پر موجود ۵ ایکٹیو نمبرز اٹھا کر کی بورڈ اپڈیٹ کریں
		activeNums := getUserActiveNumbers(userID)
		if len(activeNums) > 5 {
			activeNums = activeNums[len(activeNums)-5:]
		}

		newKB := numbersActionKB(fileName, activeNums, newVal)
		go editRawHTML(bot, chatID, msgID, cb.Message.Text, newKB)
		return
	}



	// 🟢 Admin Records Callbacks
	if strings.HasPrefix(data, "adm_flow:admin_records:") {
		if !isAdmin(cb.From.ID) {
			return
		}
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_flow:admin_records:"))
		text, kb := getAdminRecordsTextAndKB(page)
		editRawHTML(bot, chatID, msgID, text, kb)
		return
	}

	if strings.HasPrefix(data, "adm_rec_page:") {
		if !isAdmin(cb.From.ID) {
			return
		}
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_rec_page:"))
		text, kb := getAdminRecordsTextAndKB(page)
		editRawHTML(bot, chatID, msgID, text, kb)
		return
	}
	
	

    if strings.HasPrefix(data, "top_weekly_page:") {
    	page, _ := strconv.Atoi(strings.TrimPrefix(data, "top_weekly_page:"))
    	text, kb := getWeeklyTopUsersTextAndKB(page)
    	go editRawHTML(bot, chatID, msgID, text, kb)
    	return
    }

		// API Page Switch Handler
	if strings.HasPrefix(data, "adm_apis_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_apis_page:"))
		if page < 1 {
			page = 1
		}
		go editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>API Endpoint Infrastructure Access</b>"), adminManageApisKB(page))
		return
	}
	
		// ── Withdraw Account Setup Callbacks ──────────────────────────────────────
	if data == "menu:setup_wd_acc" {
		text := txt("{E_MONEY} <b>Select Withdrawal Currency:</b>\n\nChoose your preferred payout method:")
		kb := styledInlineKeyboardMarkup{
			InlineKeyboard: [][]styledInlineKeyboardButton{
				{
					{Text: "PKR (EasyPaisa/JazzCash/Bank)", CallbackData: "wd_acc_curr:pkr", IconCustomEmojiID: ID_PTICK, Style: "success"},
				},
				{
					{Text: "USD (USDT Crypto Wallet)", CallbackData: "wd_acc_curr:usd", IconCustomEmojiID: ID_USD, Style: "primary"},
				},
			},
		}
		go editRawHTML(bot, chatID, msgID, text, kb)
		return
	}
	
	// ── Preferred Currency Selection Callback ─────────────────────────────────
	// ── Preferred Currency Selection Callback ─────────────────────────────────
	if strings.HasPrefix(data, "set_pref_curr:") {
		curr := strings.TrimPrefix(data, "set_pref_curr:")
		userID := cb.From.ID

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": userID}, bson.M{
			"$set": bson.M{"currency": curr},
		})
		cancel()

		welcomeText := txt("{E_CROWN} <b>Currency Set to %s!</b>\n\n"+
			"{E_MOBILE} High quality virtual numbers available instantly.\n"+
			"{E_STAR} Easy earning and fast withdrawals.", strings.ToUpper(curr))

		go editRawHTML(bot, chatID, msgID, welcomeText, nil)

		// 🟢 dynamic constants + "Click Here" hyperlinks
		menuText := txt("{E_ADMIN} Main Menu \n {E_PTICK1}Welcome To Real Ean Bot {E_HEART} \n {E_MONEY} Keep Earning And Fast Withdrawals {E_DOLLAR} \n {E_WARN} Join Community \n {E_A1} Main Channel: <a href=\"%s\">Click Here</a> \n {E_A1} OTPs Group: <a href=\"%s\">Click Here</a> \n {E_A1} Withdraw proof: <a href=\"%s\">Click Here</a>",
			MainChannelLink, OtpGroupInviteLink, WithdrawProofsLink)

		go sendRawHTML(bot, chatID, menuText, smartKB(userID))
		return
	}


	
	
	if data == "user:initiate_wd_v2" {
		u := getOrCreateUser(cb.From, 0)

		stateMu.Lock()
		userState[cb.From.ID] = "await_wd_amount_v2"
		stateMu.Unlock()

		if u.WdAccount.Currency == "usd" {
			maxUSD := u.Balance / FixedUSDRate
			go sendRawHTML(bot, chatID, txt("{E_MONEY} <b>Enter Cashout Amount in USD ($):</b>\n\nMax Available: <b>%.2f $</b>\n<i>Example: <code>5.5</code> or <code>10</code></i>", maxUSD), nil)
		} else {
			go sendRawHTML(bot, chatID, txt("{E_MONEY} <b>Enter Cashout Amount in PKR (Rs):</b>\n\nMax Available: <b>%.2f Rs</b>\n<i>Example: <code>500</code> or <code>1000</code></i>", u.Balance), nil)
		}
		return
	}
	

	if strings.HasPrefix(data, "wd_acc_curr:") {
		curr := strings.TrimPrefix(data, "wd_acc_curr:")
		userID := cb.From.ID

		if curr == "usd" {
			text := txt("{E_LINK} <b>Select Crypto Network Protocol:</b>")
			kb := styledInlineKeyboardMarkup{
				InlineKeyboard: [][]styledInlineKeyboardButton{
					{
						{Text: "TRC20 (TRON)", CallbackData: "wd_acc_net:trc20", IconCustomEmojiID: ID_LINK, Style: "primary"},
						{Text: "BEP20 (BNB Smart Chain)", CallbackData: "wd_acc_net:bep20", IconCustomEmojiID: ID_LINK, Style: "success"},
					},
				},
			}
			go editRawHTML(bot, chatID, msgID, text, kb)
		} else {
			// PKR Selected -> Ask Bank Name
			stateMu.Lock()
			userState[userID] = "await_wd_bank_name"
			stateMu.Unlock()

			go sendRawHTML(bot, chatID, txt("{E_GEAR} <b>Please Enter Your Bank or Wallet Name:</b>\n\n<i>Example: EasyPaisa, JazzCash, Meezan Bank, SadaPay</i>"), nil)
		}
		return
	}

	if strings.HasPrefix(data, "wd_acc_net:") {
		net := strings.TrimPrefix(data, "wd_acc_net:")
		userID := cb.From.ID

		stateMu.Lock()
		userState[userID] = "await_wd_wallet_address:" + net
		stateMu.Unlock()

		go sendRawHTML(bot, chatID, txt("{E_LINK} <b>Please Enter Your Correct %s Wallet Address:</b>", strings.ToUpper(net)), nil)
		return
	}
	
	
	if strings.HasPrefix(data, "adm_prices_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_prices_page:"))
		if page < 1 { page = 1 }
		go editRawHTML(bot, chatID, msgID, txt("{E_GEAR} <b>Configured Price Rates</b>\n\nClick any price button below to remove override:"), adminManagePricesKB(page))
		return
	}

	if strings.HasPrefix(data, "adm_del_price:") {
		if !isAdmin(cb.From.ID) { return }
		targetKey := strings.TrimPrefix(data, "adm_del_price:")
		deleteCustomPrice(targetKey)
		editRawHTML(bot, chatID, msgID, txt("{E_TICK} <b>Price for %s reset to default.</b>", strings.Title(targetKey)), adminManagePricesKB(1))
		return
	}
	

	// ── IVAS Callbacks ──────────────────────────────────────────────────────────
	if data == "adm_flow:manage_ivas" {
		if !isAdmin(cb.From.ID) { return }
		go editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>IVAS Dedicated Numbers Manager</b>\n\nSelect a country below to view & manage its ranges:"), adminIvasManageKB())
		return
	}

	if strings.HasPrefix(data, "ivas_view_c:") {
		if !isAdmin(cb.From.ID) { return }
		cName := strings.TrimPrefix(data, "ivas_view_c:")
		text := txt("{E_GEAR} <b>Managing Ranges for Country: %s</b>\n\nClick on any range to set its price or click red button to delete range:", cName)
		go editRawHTML(bot, chatID, msgID, text, adminIvasCountryRangesKB(cName))
		return
	}

	if strings.HasPrefix(data, "ivas_del_range:") {
		if !isAdmin(cb.From.ID) { return }
		fileName := strings.TrimPrefix(data, "ivas_del_range:")
		cName := cleanCountryName(fileName)

		// 1. Disk & RAM Delete
		_ = os.Remove(filepath.Join(FilesDir, fileName))
		ramNumbersMu.Lock()
		delete(ramNumbers, fileName)
		ramNumbersMu.Unlock()

		// 2. Check remaining ranges for this country
		remaining := getIvasCountryRanges(cName)
		if len(remaining) == 0 {
			// اگر کوئی رینج نہ بچے تو کنٹری خودبخود مینو سے ختم اور مین IVAS مینو میں واپسی
			go editRawHTML(bot, chatID, msgID, txt("{E_TICK} <b>Range '%s' deleted. All ranges for %s removed! Country deleted automatically.</b>", fileName, cName), adminIvasManageKB())
		} else {
			// اگر ابھی رینجز باقی ہیں تو رینجز والا مینو اپڈیٹ کر دیں
			go editRawHTML(bot, chatID, msgID, txt("{E_TICK} <b>Range '%s' deleted successfully!</b>", fileName), adminIvasCountryRangesKB(cName))
		}
		return
	}

	if data == "ivas_add_country_trig" {
		if !isAdmin(cb.From.ID) { return }
		stateMu.Lock()
		userState[cb.From.ID] = "ivas_await_upload"
		stateMu.Unlock()
		sendRawHTML(bot, chatID, txt("{E_DASH} <b>Please upload your IVAS File (.xlsx, .zip, or .txt):</b>\n\nSystem will automatically extract all countries, split ranges, and create menu buttons!"), nil)
		return
	}

	if strings.HasPrefix(data, "ivas_set_price:") {
		if !isAdmin(cb.From.ID) { return }
		fName := strings.TrimPrefix(data, "ivas_set_price:")
		rName := strings.TrimSuffix(fName, ".txt")
		stateMu.Lock()
		userState[cb.From.ID] = "ivas_await_price:" + rName
		stateMu.Unlock()
		sendRawHTML(bot, chatID, txt("{E_MONEY} Send the price per OTP for Range <b>%s</b> (e.g. <code>5.0</code>):", rName), nil)
		return
	}

	
	if strings.HasPrefix(data, "adm_services_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_services_page:"))
		if page < 1 { page = 1 }
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Active Services Dashboard</b>"), adminManageServicesKB(page))
		return
	}

	if strings.HasPrefix(data, "adm_ranges_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_ranges_page:"))
		if page < 1 { page = 1 }
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Active Ranges Control Dashboard</b>"), adminActiveRangesKB(page))
		return
	}

	if strings.HasPrefix(data, "adm_unpaid_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_unpaid_page:"))
		if page < 1 { page = 1 }
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Unpaid Services Dashboard</b>"), adminManageUnpaidKB(page))
		return
	}
	

	if strings.HasPrefix(data, "ivas_delete_c:") {
		if !isAdmin(cb.From.ID) { return }
		fName := strings.TrimPrefix(data, "ivas_delete_c:")
		_ = os.Remove(filepath.Join(FilesDir, fName))
		go editRawHTML(bot, chatID, msgID, txt("{E_TICK} IVAS Country file deleted."), adminIvasManageKB())
		return
	}

	if data == "ivas_add_country_trig" {
		if !isAdmin(cb.From.ID) { return }
		stateMu.Lock()
		userState[cb.From.ID] = "ivas_await_upload"
		stateMu.Unlock()
		sendRawHTML(bot, chatID, txt("{E_DASH} <b>Please upload your IVAS File (.xlsx, .zip, or .txt):</b>\n\nSystem will automatically extract all countries, split ranges, and create menu buttons!"), nil)
		return
	}


	if strings.HasPrefix(data, "ivas_append_nums:") {
		if !isAdmin(cb.From.ID) { return }
		fName := strings.TrimPrefix(data, "ivas_append_nums:")
		stateMu.Lock()
		userState[cb.From.ID] = "ivas_await_append_file:" + fName
		stateMu.Unlock()
		sendRawHTML(bot, chatID, txt("{E_DASH} Send the text or numbers to add to IVAS file <code>%s</code>:", fName), nil)
		return
	}

	if strings.HasPrefix(data, "ivas_set_price:") {
		if !isAdmin(cb.From.ID) { return }
		fName := strings.TrimPrefix(data, "ivas_set_price:")
		cName := strings.TrimSuffix(fName, "0.txt")
		stateMu.Lock()
		userState[cb.From.ID] = "ivas_await_price:" + cName
		stateMu.Unlock()
		sendRawHTML(bot, chatID, txt("{E_MONEY} Send the price per OTP for IVAS <b>%s</b> (e.g. <code>5.0</code>):", cName), nil)
		return
	}

	
	if strings.HasPrefix(data, "country_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "country_page:"))
		if page < 1 { page = 1 }
		go editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Country:</b>"), countriesInlineKB(page))
		return
	}
	if strings.HasPrefix(data, "active_country_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "active_country_page:"))
		if page < 1 { page = 1 }
		editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Active Range Country:</b>"), activeCountriesInlineKB(page))
		return
	}
	if data == "menu:getnum_active" {
		editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Active Range Country:</b>"), activeCountriesInlineKB(1))
		return
	}
	if data == "menu:getnum_all" {
		editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Country:</b>"), countriesInlineKB(1))
		return
	}
	if data == "menu:getnum_apps" {
		editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Active Service:</b>"), activeServicesInlineKB(1))
		return
	}
	if strings.HasPrefix(data, "services_page:") {
		page, _ := strconv.Atoi(strings.TrimPrefix(data, "services_page:"))
		if page < 1 { page = 1 }
		editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Active Service:</b>"), activeServicesInlineKB(page))
		return
	}
	if strings.HasPrefix(data, "adm_del_range:") {
		if !isAdmin(cb.From.ID) { return }
		removeActiveRange(strings.TrimPrefix(data, "adm_del_range:"))
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Active Ranges Control Dashboard</b>\n\nClick any existing entry button below to delete it."), adminActiveRangesKB(1))
		return
	}
	if strings.HasPrefix(data, "adm_del_unpaid:") {
		if !isAdmin(cb.From.ID) { return }
		removeUnpaidService(strings.TrimPrefix(data, "adm_del_unpaid:"))
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Unpaid Services Dashboard</b>\n\nClick any existing entry below to delete it."), adminManageUnpaidKB(1))
		return
	}
	if data == "menu:back_to_admin" {
		if !isAdmin(cb.From.ID) { return }
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Premium Control Center Admin Panel</b>\n\nWelcome back administrator! Manage configurations below."), adminKB(1))
		return
	}

	if strings.HasPrefix(data, "adm_del_admin:") {
		if !isAdmin(cb.From.ID) { return }
		targetID, _ := strconv.ParseInt(strings.TrimPrefix(data, "adm_del_admin:"), 10, 64)
		configMu.Lock()
		var updated []int64
		for _, id := range AdminIDs {
			if id != targetID { updated = append(updated, id) }
		}
		if len(updated) > 0 { AdminIDs = updated }
		configMu.Unlock()
		syncConfigToDB()
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Administrators Access Registry</b>"), adminActiveRangesKB(1))
		return
	}
	
	if strings.HasPrefix(data, "adm_del_api:") {
		if !isAdmin(cb.From.ID) { return }
		targetIdx, _ := strconv.Atoi(strings.TrimPrefix(data, "adm_del_api:"))
		
		configMu.Lock()
		if targetIdx >= 0 && targetIdx < len(API_Bases) {
			files, err := os.ReadDir(FilesDir)
			if err == nil {
				targetSuffix := fmt.Sprintf("%d.txt", targetIdx+1)
				for _, f := range files {
					if !f.IsDir() && strings.HasSuffix(f.Name(), targetSuffix) {
						os.Remove(filepath.Join(FilesDir, f.Name()))
					}
				}
				for i := targetIdx + 1; i < len(API_Bases); i++ {
					oldSuffix := fmt.Sprintf("%d.txt", i+1)
					newSuffix := fmt.Sprintf("%d.txt", i)
					for _, f := range files {
						if !f.IsDir() && strings.HasSuffix(f.Name(), oldSuffix) {
							oldPath := filepath.Join(FilesDir, f.Name())
							newPath := filepath.Join(FilesDir, strings.TrimSuffix(f.Name(), oldSuffix)+newSuffix)
							os.Rename(oldPath, newPath)
						}
					}
				}
			}
			API_Bases = append(API_Bases[:targetIdx], API_Bases[targetIdx+1:]...)
		}
		configMu.Unlock()
		syncConfigToDB()
		
		editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>API Endpoint Infrastructure Access</b>"), adminManageApisKB(1))
		return
	}

	if strings.HasPrefix(data, "country_select:") {
		rawTarget := strings.TrimPrefix(data, "country_select:")
		userID := cb.From.ID

		baseTarget := strings.TrimSuffix(rawTarget, ".txt")
		if idx := strings.Index(baseTarget, "-"); idx != -1 {
			baseTarget = baseTarget[:idx]
		}

		validNums := getValidNumbersForTarget(baseTarget, "")
		if len(validNums) == 0 {
			go bot.Request(tgbotapi.NewCallbackWithAlert(cb.ID, "Cool Down."))
			return
		}

		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		r.Shuffle(len(validNums), func(i, j int) { validNums[i], validNums[j] = validNums[j], validNums[i] })

		var selected []NumberWithFile
		limit := 5
		if len(validNums) < 5 {
			limit = len(validNums)
		}
		for i := 0; i < limit; i++ {
			selected = append(selected, validNums[i])
		}

		// 🟢 بیک اینڈ میں ۱۰ سے پرانے ۵ ان لاک ہو جائیں گے
		enforceUserLockLimit(userID, len(selected))

		now := time.Now()
		var numStrings []string

		ramUserLocksMu.Lock()
		for _, item := range selected {
			numStrings = append(numStrings, item.Phone)
			ramUserLocks[item.Phone] = RAMLockInfo{UserID: userID, CountryFile: item.FileName, LockedAt: now}
		}
		ramUserLocksMu.Unlock()

		go func(uid int64, items []NumberWithFile, t time.Time) {
			for _, item := range items {
				sqliteDB.Exec("INSERT OR REPLACE INTO user_locks (user_id, phone_number, country_file, locked_at) VALUES (?, ?, ?, ?)", uid, item.Phone, item.FileName, t)
			}
		}(userID, selected, now)

		allocatedPrice := getPriceForCountry(baseTarget)
		u := getOrCreateUser(cb.From, 0)
		curr := getUserCurrency(u)
		priceStr := formatPrice(allocatedPrice, curr)

		// 🟢 یوزر کی سیٹنگ ریم سے اٹھائیں
		userWithoutCCMu.RLock()
		withoutCC := userWithoutCC[userID]
		userWithoutCCMu.RUnlock()

		timeNowStr := time.Now().Format("03:04:05 PM")
		text := txt("{E_MOBILE} <b>NUMBERS ALLOCATED</b>\n\nYour Numbers Are Here. Tap To Copy.\n\n<b>Per OTP Price:</b> %s\n⏱️ <i>Refreshed: %s</i>\n\nOTP Will Come Here.", priceStr, timeNowStr)

		// ⚡ ۵ تازہ نمبرز یوزر کی سیٹنگ کے مطابق ڈسپلے ہوں گے
		go editRawHTML(bot, chatID, msgID, text, numbersActionKB(baseTarget, numStrings, withoutCC))
		return

	}




	if strings.HasPrefix(data, "app_cselect:") {
		rawTarget := strings.TrimPrefix(data, "country_select:")
		userID := cb.From.ID

		baseTarget := strings.TrimSuffix(rawTarget, ".txt")
		if idx := strings.Index(baseTarget, "-"); idx != -1 {
			baseTarget = baseTarget[:idx]
		}

		validNums := getValidNumbersForTarget(baseTarget, "")
		if len(validNums) == 0 {
			go bot.Request(tgbotapi.NewCallbackWithAlert(cb.ID, "Cool Down."))
			return
		}

		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		r.Shuffle(len(validNums), func(i, j int) { validNums[i], validNums[j] = validNums[j], validNums[i] })

		var selected []NumberWithFile
		limit := 5
		if len(validNums) < 5 {
			limit = len(validNums)
		}
		for i := 0; i < limit; i++ {
			selected = append(selected, validNums[i])
		}

		// 🟢 بیک اینڈ میں ۱۰ سے پرانے ۵ ان لاک ہو جائیں گے
		enforceUserLockLimit(userID, len(selected))

		now := time.Now()
		var numStrings []string

		ramUserLocksMu.Lock()
		for _, item := range selected {
			numStrings = append(numStrings, item.Phone)
			ramUserLocks[item.Phone] = RAMLockInfo{UserID: userID, CountryFile: item.FileName, LockedAt: now}
		}
		ramUserLocksMu.Unlock()

		go func(uid int64, items []NumberWithFile, t time.Time) {
			for _, item := range items {
				sqliteDB.Exec("INSERT OR REPLACE INTO user_locks (user_id, phone_number, country_file, locked_at) VALUES (?, ?, ?, ?)", uid, item.Phone, item.FileName, t)
			}
		}(userID, selected, now)

		allocatedPrice := getPriceForCountry(baseTarget)
		u := getOrCreateUser(cb.From, 0)
		curr := getUserCurrency(u)
		priceStr := formatPrice(allocatedPrice, curr)

		// 🟢 یوزر کی سیٹنگ ریم سے اٹھائیں
		userWithoutCCMu.RLock()
		withoutCC := userWithoutCC[userID]
		userWithoutCCMu.RUnlock()

		timeNowStr := time.Now().Format("03:04:05 PM")
		text := txt("{E_MOBILE} <b>NUMBERS ALLOCATED</b>\n\nYour Numbers Are Here. Tap To Copy.\n\n<b>Per OTP Price:</b> %s\n⏱️ <i>Refreshed: %s</i>\n\nOTP Will Come Here.", priceStr, timeNowStr)

		// ⚡ ۵ تازہ نمبرز یوزر کی سیٹنگ کے مطابق ڈسپلے ہوں گے
		go editRawHTML(bot, chatID, msgID, text, numbersActionKB(baseTarget, numStrings, withoutCC))
		return

	}




	if strings.HasPrefix(data, "app_select:") {
		appName := strings.TrimPrefix(data, "app_select:")
		editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Country for %s</b>", appName), appCountriesInlineKB(appName, 1))
		return
	}
	if strings.HasPrefix(data, "appc_page:") {
		parts := strings.SplitN(data, ":", 3)
		if len(parts) < 3 { return }
		appName := parts[1]
		page, _ := strconv.Atoi(parts[2])
		if page < 1 { page = 1 }
		editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Country for %s</b>", appName), appCountriesInlineKB(appName, page))
		return
	}


	if data == "menu:change_country" {
		editRawHTML(bot, chatID, msgID, txt("{E_CHANNEL} <b>Select Allocation Route Option:</b>"), getNumberRoutingKB())
		return
	}
	if data == "menu:main_close" {
		editRawHTML(bot, chatID, msgID, txt("{E_TICK} Session closed."), nil)
		return
	}
	
	if data == "user:initiate_wd" {
		u := getOrCreateUser(cb.From, 0) // 👈 cb.From.ID کی جگہ cb.From کر دیا گیا ہے

		// 1. Time Check
		if !isWithdrawTime() {
			sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Withdrawal Window Closed!</b>\nWithdrawal timing is strictly from 5:00 PM to 8:00 PM PKT."), nil)
			return
		}

		// 2. Balance Check
		if u.Balance < MinWithdrawAmount {
			sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Insufficient Balance!</b>\nMinimum required threshold is %.0f %s.", MinWithdrawAmount, CurrencySymbol), nil)
			return
		}

		stateMu.Lock()
		userState[cb.From.ID] = "await_wd_amount"
		stateMu.Unlock()
		sendRawHTML(bot, chatID, txt("{E_MONEY} Enter cashout amount:"), nil)
		return
	}




	// Admin service management
	if strings.HasPrefix(data, "adm_flow:") {
		if !isAdmin(cb.From.ID) { return }
		action := strings.TrimPrefix(data, "adm_flow:")
		switch action {
		case "broadcast":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_broadcast"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_MOBILE} Send broadcast payload message text:"), nil)
		case "manage_active_ranges":
			editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Active Ranges Control Dashboard</b>\n\nClick any existing entry button below to delete it."), adminActiveRangesKB(1))
		case "manage_active_apps":
			editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Active Services Dashboard</b>\n\nClick a service to edit, add country, or delete."), adminManageServicesKB(1))
		case "manage_unpaid":
			editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Unpaid Services Dashboard</b>\n\nClick any existing entry below to delete it."), adminManageUnpaidKB(1))
		case "add_app_trigger":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_new_app"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("Send the service name and optional premium icon ID separated by a space.\nExample:\n<code>Telegram 6237864166879663987</code>\nOr just <code>WhatsApp</code> for default icon."), nil)
		case "add_unpaid_trigger":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_unpaid_app"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_DASH} Send the exact service name to mark as unpaid (e.g. <code>X App</code>):"), nil)
		case "manage_admins":
			editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>Administrators Access Registry</b>"), adminManageAdminsKB())
		case "manage_apis":
			editRawHTML(bot, chatID, msgID, txt("{E_ADMIN} <b>API Endpoint Infrastructure Access</b>"), adminManageApisKB(1))
		case "add_admin_trigger":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_new_admin"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_USERS} Provide Telegram User ID to declare as System Administrator:"), nil)
		case "add_api_trigger":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_new_api"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_CHANNEL} Input Smart API Base URL string (System attaches tracking query formats automatically):"), nil)
		case "add_active_range_trigger":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_active_range"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_DASH} Input Range Identifier String (e.g. <code>Pakistan1</code>):"), nil)
		case "manage_price":
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var res bson.M
			_ = statsCollection.FindOne(ctx, bson.M{"id": "config"}).Decode(&res)
			defaultPriceVal := 1.0
			if val, ok := res["otp_price"]; ok {
				if fVal, ok := val.(float64); ok { defaultPriceVal = fVal }
			}
			text := txt("{E_GEAR} <b>Rate Setup Dashboard</b>\n\n"+
				"<b>Default Price (Global):</b> %.2f %s\n\n"+
				"Below are configured custom price overrides. Click any button to delete it:", defaultPriceVal, CurrencySymbol)
			editRawHTML(bot, chatID, msgID, text, adminManagePricesKB(1))

		case "set_price_trigger":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_price"
			stateMu.Unlock()
			helpInstruction := "<b>Set New Price Configuration Parameters</b>\n\n" +
				"• Send single numeric value to update global default price (e.g. <code>2.5</code>)\n" +
				"• Send country text identifier and number to bind custom rates to specific targets (e.g. <code>Pakistan 3</code>)"
			sendRawHTML(bot, chatID, helpInstruction, nil)
			
		case "manage_withdraw":
			minWd := getMinWithdrawAmount()
			_, _, timeStr := getWithdrawTimeConfig()
			text := txt("{E_GEAR} <b>Withdrawal Management Panel</b>\n\n"+
				"{E_MONEY} <b>Current Minimum Withdraw:</b> %.0f %s\n"+
				"{E_PTICK1} <b>Current Withdraw Time:</b> %s (PKT)\n\n"+
				"Choose an option below to update settings:", minWd, CurrencySymbol, timeStr)
			editRawHTML(bot, chatID, msgID, text, adminManageWithdrawKB())

		case "set_min_wd_trig":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_min_wd"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_MONEY} <b>Enter new Minimum Withdraw amount:</b>\nExample: <code>100</code> or <code>200</code>"), nil)

		case "set_wd_time_trig":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_wd_time"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_GEAR} <b>Enter Withdraw Time range separated by space:</b>\nExample: <code>5PM 8PM</code> or <code>5PM 10PM</code>"), nil)


		case "check_user_trig":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_check_user_id"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_USERS} <b>Enter Target Telegram User ID to inspect details:</b>"), nil)

		case "add_record_trig":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_add_record"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_RECEIPT} <b>Send your accounting record text payload to store:</b>"), nil)

		case "del_record_trig":
			stateMu.Lock()
			userState[cb.From.ID] = "adm_await_del_record"
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_TRASH} <b>Send Record ID number to delete (e.g. <code>3</code>):</b>"), nil)
			
			
		case "live_stats":
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			totalUsers, _ := usersCollection.CountDocuments(ctx, bson.M{})
			dayAgo := time.Now().Add(-24 * time.Hour)
			activeToday, _ := usersCollection.CountDocuments(ctx, bson.M{"last_active": bson.M{"$gte": dayAgo}})
			pipeline := mongo.Pipeline{{{"$group", bson.M{"_id": nil, "total_liability": bson.M{"$sum": "$balance"}}}}}
			cursor, _ := usersCollection.Aggregate(ctx, pipeline)
			var agg []bson.M
			var totalLiability float64 = 0.0
			if err := cursor.All(ctx, &agg); err == nil && len(agg) > 0 { totalLiability = agg[0]["total_liability"].(float64) }
			g := getGlobalStats()
			
			displayTodayOTPs := g.TodayOTPsReceived
			if g.LastOTPDate.In(time.Local).Format("2006-01-02") != time.Now().In(time.Local).Format("2006-01-02") {
				displayTodayOTPs = 0
			}

			var totalFilesNumbers int = 0
			files, _ := os.ReadDir(FilesDir)
			for _, f := range files {
				if !f.IsDir() {
					b, _ := os.ReadFile(filepath.Join(FilesDir, f.Name()))
					totalFilesNumbers += len(strings.Split(string(b), "\n"))
				}
			}
			text := txt("{E_STATS} <b>Dashboard Metrics Engine</b>\n\n"+
				"{E_USERS} Total Users: <b>%d</b>\n"+
				"{E_TICK} Active Today: <b>%d</b>\n\n"+
				"{E_MOBILE} Total Numbers Loaded: <b>%d</b>\n"+
				"{E_STATS} Total OTPs Processed: <b>%d</b>\n"+
				"{E_STAR} Today OTPs Processed: <b>%d</b>\n\n"+
				"{E_MONEY} System Liabilities: <b>%.2f %s</b>\n"+
				"{E_GEAR} Total Withdrawn Overall: <b>%.2f %s</b>",
				totalUsers, activeToday, totalFilesNumbers, g.TotalOTPsReceived, displayTodayOTPs, totalLiability, CurrencySymbol, g.TotalWithdrawn, CurrencySymbol)
			editRawHTML(bot, chatID, msgID, text, adminKB(1))
		}
		return
	}

	if strings.HasPrefix(data, "app_adm:") {
		if !isAdmin(cb.From.ID) { return }
		appName := strings.TrimPrefix(data, "app_adm:")
		editRawHTML(bot, chatID, msgID, txt("{E_GEAR} <b>Manage Service: %s</b>", appName), adminEditServiceKB(appName))
		return
	}
	if strings.HasPrefix(data, "app_delcountry:") {
		if !isAdmin(cb.From.ID) { return }
		parts := strings.SplitN(data, ":", 3)
		if len(parts) == 3 {
			appName := parts[1]
			country := parts[2]
			removeCountryFromApp(appName, country)
			editRawHTML(bot, chatID, msgID, txt("{E_GEAR} <b>Manage Service: %s</b>", appName), adminEditServiceKB(appName))
		}
		return
	}
	if strings.HasPrefix(data, "app_edit:") {
		if !isAdmin(cb.From.ID) { return }
		appName := strings.TrimPrefix(data, "app_edit:")
		stateMu.Lock()
		userState[cb.From.ID] = "adm_await_edit_app:" + appName
		stateMu.Unlock()
		sendRawHTML(bot, chatID, fmt.Sprintf("Send new name and optional icon ID for service <b>%s</b> (space separated):\n<code>NewName [icon_id]</code>", appName), nil)
		return
	}
	if strings.HasPrefix(data, "app_addcountry:") {
		if !isAdmin(cb.From.ID) { return }
		appName := strings.TrimPrefix(data, "app_addcountry:")
		stateMu.Lock()
		userState[cb.From.ID] = "adm_await_add_app_country:" + appName
		stateMu.Unlock()
		sendRawHTML(bot, chatID, fmt.Sprintf("Send the country identifier (e.g. <code>Pakistan1</code>) to add to service <b>%s</b>:", appName), nil)
		return
	}
	if strings.HasPrefix(data, "app_delete:") {
		if !isAdmin(cb.From.ID) { return }
		appName := strings.TrimPrefix(data, "app_delete:")
		deleteAppByName(appName)
		editRawHTML(bot, chatID, msgID, txt("{E_TICK} Service deleted. Returning to Active Services."), adminManageServicesKB(1))
		return
	}

	if strings.HasPrefix(data, "adm_wd:") && isAdmin(cb.From.ID) {
		parts := strings.Split(data, ":")
		action := parts[1]
		targetUID, _ := strconv.ParseInt(parts[2], 10, 64)
		amount, _ := strconv.ParseFloat(parts[3], 64) // Amount is in PKR internally
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var u UserData
		err := usersCollection.FindOne(ctx, bson.M{"id": targetUID}).Decode(&u)
		originalText := cb.Message.Text
		safeText := html.EscapeString(originalText)
		paymentDetails := "Unknown"
		splitText := strings.Split(originalText, "Account Payment Details:")
		if len(splitText) > 1 { paymentDetails = strings.TrimSpace(splitText[1]) }
		maskedPaymentDetails := maskAccount(paymentDetails)
		userDisplay := fmt.Sprintf("<code>%d</code>", u.ID)
		if u.Username != "" { userDisplay = fmt.Sprintf("@%s", u.Username) }

		curr := getUserCurrency(u)
		amountDisplay := formatBalance(amount, curr)

		if err == nil && action == "approve" {
			// 🟢 وڈرا اپروو ہونے پر سائیکل کاؤنٹرز زیرو کر دیں
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUID}, bson.M{
				"$inc": bson.M{"total_withdrawn": amount},
				"$set": bson.M{
					"api_cycle_otps":     bson.M{}, // 🔄 Reset Cycle OTPs to 0
					"api_cycle_earnings": bson.M{}, // 🔄 Reset Cycle Earnings to 0
				},
			})
			cancel()

			globalStats := getGlobalStats()
			globalStats.TotalWithdrawn += amount
			updateGlobalStats(globalStats)

			go sendRawHTML(bot, targetUID, txt("{E_TICK} <b>Withdraw Approved!</b> %s transferred.", amountDisplay), smartKB(targetUID))
			newAdminText := txt("<b>[ REQUEST EVALUATED ]</b>\n") + safeText + txt("\n\n{E_TICK} <b>STATUS: APPROVED ✅</b>")
			go editRawHTML(bot, chatID, msgID, newAdminText, nil)
			groupNotifyText := txt("{E_TICK} <b>Withdrawal Successfully Approved!</b>\n\n"+
				"{E_USERS} User: <b>%s</b>\n"+
				"{E_ADMIN} Name: <b>%s</b>\n"+
				"{E_MONEY} Amount: <b>%s</b>\n"+
				"{E_MOBILE} Account: <code>%s</code>\n\n"+
				"{E_CROWN} <i>Keep Earning with Premium OTP Bot!</i>",
				userDisplay, u.FirstName, amountDisplay, maskedPaymentDetails)
			go sendRawHTML(bot, OtpGroupId, groupNotifyText, nil)

		} else if err == nil && action == "decline" {
			_, _ = usersCollection.UpdateOne(ctx, bson.M{"id": targetUID}, bson.M{"$inc": bson.M{"balance": amount}})

			sendRawHTML(bot, targetUID, txt("{E_CROSS} <b>Withdraw Declined!</b> %s returned to account balance.", amountDisplay), smartKB(targetUID))
			newAdminText := txt("<b>[ REQUEST EVALUATED ]</b>\n") + safeText + txt("\n\n{E_CROSS} <b>STATUS: DECLINED ❌</b>")
			go editRawHTML(bot, chatID, msgID, newAdminText, nil)
			groupNotifyText := txt("{E_CROSS} <b>Withdrawal Declined / Refunded!</b>\n\n"+
				"{E_USERS} User: <b>%s</b>\n"+
				"{E_ADMIN} Name: <b>%s</b>\n"+
				"{E_MONEY} Amount: <b>%s</b>\n"+
				"{E_WARN} Status: <b>Returned to Balance</b>",
				userDisplay, u.FirstName, amountDisplay)
			sendRawHTML(bot, OtpGroupId, groupNotifyText, nil)
		}
	}
}

// 🟢 Admin Record System Model
type AdminRecord struct {
	ID        int       `bson:"id" json:"id"`
	Text      string    `bson:"text" json:"text"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

var adminRecordsCollection *mongo.Collection


type NumberWithFile struct {
	Phone    string
	FileName string
}

// 🟢 Smart Prefix & Multi-Range Matcher
// 🟢 Smart Prefix & Multi-Range Matcher (Base Target & All Locks Exclusion)
func getValidNumbersForTarget(target string, serviceFilter string) []NumberWithFile {
	cleanTarget := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(target), ".txt"))
	
	// اگر فائل نام میں ڈیش ہو (مثلاً pakistan1-1) تو بیس نام (pakistan1) الگ کریں
	baseTarget := cleanTarget
	if idx := strings.Index(cleanTarget, "-"); idx != -1 {
		baseTarget = cleanTarget[:idx]
	}

	globalUsed := getGlobalUsedNumbers()
	var serviceUsed map[string]bool
	if serviceFilter != "" {
		serviceUsed = getServiceUsedNumbers(serviceFilter)
	}

	now := time.Now()
	activeLocks := make(map[string]bool)
	ramUserLocksMu.RLock()
	for phone, lockInfo := range ramUserLocks {
		if now.Sub(lockInfo.LockedAt) < 10*time.Minute {
			activeLocks[phone] = true // تمام ایکٹیو لاک نمبرز کو نئے پول میں انے سے روکے
		}
	}
	ramUserLocksMu.RUnlock()

	var results []NumberWithFile

	ramNumbersMu.RLock()
	for fileName, lines := range ramNumbers {
		if !strings.HasSuffix(fileName, ".txt") {
			continue
		}
		cleanFile := strings.ToLower(strings.TrimSuffix(fileName, ".txt"))

		// بیس نام یا کسی بھی سب رینج (Pakistan1, Pakistan1-1, Pakistan1-2) سے تمام دستیاب نمبر اٹھائے
		if cleanFile == cleanTarget || cleanFile == baseTarget || strings.HasPrefix(cleanFile, baseTarget+"-") {
			for _, l := range lines {
				n := strings.TrimSpace(l)
				if n == "" {
					continue
				}

				isUsed := false
				if serviceFilter != "" {
					isUsed = serviceUsed[n]
				} else {
					isUsed = globalUsed[n]
				}

				if !isUsed && !activeLocks[n] {
					results = append(results, NumberWithFile{
						Phone:    n,
						FileName: fileName,
					})
				}
			}
		}
	}
	ramNumbersMu.RUnlock()

	return results
}



// ── Helper: Check if input text is a Menu Button or Command ──────────────────
func isMenuAction(text string) bool {
	if strings.HasPrefix(text, "/") {
		return true
	}
	menuButtons := map[string]bool{
		"Get Number":   true,
		"My Account":   true,
		"Stats":        true,
		"Withdraw":     true,
		"Top Users":    true,
		"Rewards":      true,
		"Support":      true,
		"Main Channel": true,
		"Admin Panel":  true,
	}
	return menuButtons[text]
}

func handleTextMessage(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	userID := update.Message.From.ID
	chatID := update.Message.Chat.ID
	text := strings.TrimSpace(update.Message.Text)

	// 🛑 1. Automatically drop & cancel previous state if user pressed a menu button or command
	if isMenuAction(text) {
		stateMu.Lock()
		delete(userState, userID)
		delete(withdrawAmounts, userID)
		stateMu.Unlock()
	}

	stateMu.Lock()
	state, hasState := userState[userID]
	stateMu.Unlock()

	if hasState {
		switch {
		// ── IVAS Text Input Handlers (English) ────────────────────────────────
		case state == "ivas_await_country_name":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			cName := strings.TrimSpace(text)
			if cName == "" {
				go sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Action Refused: Invalid country name parameters.</b>"), smartKB(userID))
				return
			}
			stateMu.Lock()
			userState[userID] = "ivas_await_create_file:" + cName
			stateMu.Unlock()
			sendRawHTML(bot, chatID, txt("{E_DASH} Send phone numbers payload list (one number per line) or TXT string for <b>%s</b>:", cName), nil)
		
		case state == "adm_await_min_wd":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			minVal, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
			if err != nil || minVal <= 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid amount entered! Please enter a valid number.</b>"), smartKB(userID))
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			opts := options.Update().SetUpsert(true)
			_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$set": bson.M{"min_withdraw": minVal}}, opts)

			go sendRawHTML(bot, chatID, txt("{E_TICK} <b>Minimum Withdraw successfully updated to: %.0f %s</b>", minVal, CurrencySymbol), smartKB(userID))

		case state == "await_wd_amount_v2":
			amount, err := strconv.ParseFloat(text, 64)
			u := getOrCreateUser(update.Message.From, 0)
			curr := getUserCurrency(u)

			var reqPKRAmount float64
			var isValid bool
			var errorMsg string

			if curr == "usd" {
				minUSD := 1.0 // $1 Minimum strictly
				userUSD := u.Balance / FixedUSDRate

				if err == nil && amount >= minUSD && amount <= userUSD {
					isValid = true
					reqPKRAmount = amount * FixedUSDRate
				} else {
					errorMsg = txt("{E_CROSS} <b>Invalid Amount Format or Limits!</b>\n\n• Minimum Limit: <b>1.00 $</b>\n• Maximum Available: <b>%.2f $</b>\n\nPlease enter a valid number (e.g. <code>%.2f</code>):", userUSD, userUSD)
				}
			} else {
				minPKR := getMinWithdrawAmount()

				if err == nil && amount >= minPKR && amount <= u.Balance {
					isValid = true
					reqPKRAmount = amount
				} else {
					errorMsg = txt("{E_CROSS} <b>Invalid Amount Format or Limits!</b>\n\n• Minimum Limit: <b>%.0f %s</b>\n• Maximum Available: <b>%.2f %s</b>\n\nPlease enter a valid number (e.g. <code>%.0f</code>):", minPKR, CurrencySymbol, u.Balance, CurrencySymbol, u.Balance)
				}
			}

			if !isValid {
				go sendRawHTML(bot, chatID, errorMsg, nil)
				return
			}

			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			// اٹامک طریقے سے بیلنس ڈیڈکٹ کریں
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := usersCollection.UpdateOne(ctx,
				bson.M{"id": userID, "balance": bson.M{"$gte": reqPKRAmount}},
				bson.M{"$inc": bson.M{"balance": -reqPKRAmount}},
			)
			if err != nil || res.ModifiedCount == 0 {
				go sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Processing Error! Request Dropped.</b>"), smartKB(userID))
				return
			}

			userDisplay := fmt.Sprintf("<code>%d</code>", u.ID)
			if u.Username != "" { userDisplay = fmt.Sprintf("@%s", u.Username) }

			accInfo := ""
			amountDisplay := ""

			if curr == "usd" {
				amountDisplay = fmt.Sprintf("%.2f $", amount)
				accInfo = fmt.Sprintf("• Currency: USD ($)\n• Network: %s\n• Address: <code>%s</code>", strings.ToUpper(u.WdAccount.Network), u.WdAccount.WalletAddr)
			} else {
				amountDisplay = fmt.Sprintf("%.2f %s", amount, CurrencySymbol)
				accInfo = fmt.Sprintf("• Currency: PKR (Rs)\n• Bank/Wallet: %s\n• Account No: <code>%s</code>\n• Account Name: %s", u.WdAccount.BankName, u.WdAccount.AccountNo, u.WdAccount.AccountName)
			}

			earningsBreakdown := buildWithdrawalBreakdown(u)

			groupText := txt("{E_GEAR} <b>New Cashout Request Pending Evaluation</b>\n\n"+
				"{E_USERS} User: <b>%s</b>\n"+
				"{E_ADMIN} Name: <b>%s</b>\n"+
				"{E_MONEY} Amount: <b>%s</b>\n\n"+
				"💳 <b>Panel Earnings Breakdown (Cycle/Total):</b>\n<code>%s</code>\n\n"+
				"💳 <b>Account Details:</b>\n%s",
				userDisplay, u.FirstName, amountDisplay, earningsBreakdown, accInfo)


			inlineAction := styledInlineKeyboardMarkup{
				InlineKeyboard: [][]styledInlineKeyboardButton{
					{
						{Text: "Approve Payment", CallbackData: fmt.Sprintf("adm_wd:approve:%d:%f", u.ID, reqPKRAmount), IconCustomEmojiID: ID_TICK, Style: "success"},
						{Text: "Decline Payment", CallbackData: fmt.Sprintf("adm_wd:decline:%d:%f", u.ID, reqPKRAmount), IconCustomEmojiID: ID_CROSS, Style: "danger"},
					},
				},
			}

			go sendRawHTML(bot, WithdrawGroupId, groupText, inlineAction)
			go sendRawHTML(bot, chatID, txt("{E_TICK} <b>Withdrawal Request Submitted Successfully!</b>\n\nYour request of <b>%s</b> has been routed to administration.", amountDisplay), smartKB(userID))

		// 🟢 1. Check User Detail Logic
		// 🟢 1. Check User Detail Logic (Updated with Premium Emojis & Total OTPs)
		case state == "adm_await_check_user_id":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			targetUID, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
			if err != nil || targetUID <= 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid User ID. Action cancelled.</b>"), smartKB(userID))
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var u UserData
			err = usersCollection.FindOne(ctx, bson.M{"id": targetUID}).Decode(&u)
			if err != nil {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>User ID %d not found in database!</b>", targetUID), smartKB(userID))
				return
			}

			// 🟢 Audit Math Calculations
			expectedBalance := u.TotalEarned - u.TotalWithdrawn - u.TotalSpent
			mathMismatch := false
			if (u.Balance - expectedBalance) > 1.0 || (expectedBalance - u.Balance) > 1.0 {
				mathMismatch = true
			}

			auditStatus := txt("{E_TICK} <b>Calculations Normal (No Mismatch)</b>")
			if mathMismatch {
				auditStatus = txt("{E_CROSS} <b>MISMATCH DETECTED!</b> Expected: %.2f | Stored: %.2f", expectedBalance, u.Balance)
			}

			// 🟢 Panel-wise Breakdown
			panelBreakdown := buildWithdrawalBreakdown(u)

			userMention := fmt.Sprintf("<code>%d</code>", u.ID)
			if u.Username != "" {
				userMention = fmt.Sprintf("@%s", u.Username)
			}

			report := txt("{E_CROWN} <b>USER ACCOUNT AUDIT REPORT</b>\n\n"+
				"{E_USERS} <b>User:</b> %s (%s)\n"+
				"{E_ADMIN} <b>User ID:</b> <code>%d</code>\n"+
				"{E_LOAD1} <b>Joined:</b> %s\n\n"+
				"{E_MOBILE} <b>Total OTPs:</b> <b>%d</b>\n"+
				"{E_MONEY} <b>Current Balance:</b> <b>%.2f %s</b>\n"+
				"{E_STAR} <b>Total Earned:</b> <b>%.2f %s</b>\n"+
				"{E_GEAR} <b>Total Withdrawn:</b> <b>%.2f %s</b>\n"+
				"{E_GIFT} <b>Referral Earnings:</b> <b>%.2f %s</b>\n\n"+
				"{E_STATS} <b>Balance Audit Status:</b>\n%s\n\n"+
				"{E_RECEIPT} <b>Panel Wise OTPs & Earnings (Cycle/Total):</b>\n<code>%s</code>",
				u.FirstName, userMention, u.ID, u.JoinedAt.Format("2006-01-02"),
				u.TotalOTPs,
				u.Balance, CurrencySymbol, u.TotalEarned, CurrencySymbol,
				u.TotalWithdrawn, CurrencySymbol, u.ReferralEarningsEarned, CurrencySymbol,
				auditStatus, panelBreakdown)

			sendRawHTML(bot, chatID, report, smartKB(userID))


		// 🟢 2. Add Admin Record Logic
		case state == "adm_await_add_record":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			cleanRecordText := strings.TrimSpace(text)
			if cleanRecordText == "" {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Record text cannot be empty!</b>"), smartKB(userID))
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			count, _ := adminRecordsCollection.CountDocuments(ctx, bson.M{})
			nextID := int(count) + 1

			rec := AdminRecord{
				ID:        nextID,
				Text:      cleanRecordText,
				CreatedAt: time.Now(),
			}

			_, err := adminRecordsCollection.InsertOne(ctx, rec)
			if err != nil {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Failed to store record: %v</b>", err), smartKB(userID))
				return
			}

			recText, kb := getAdminRecordsTextAndKB(1)
			sendRawHTML(bot, chatID, txt("{E_TICK} <b>Record #%d saved successfully!</b>\n\n", nextID)+recText, kb)

		// 🟢 3. Delete Admin Record Logic
		case state == "adm_await_del_record":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			targetID, err := strconv.Atoi(strings.TrimSpace(text))
			if err != nil || targetID <= 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid Record ID number.</b>"), smartKB(userID))
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := adminRecordsCollection.DeleteOne(ctx, bson.M{"id": targetID})
			if err != nil || res.DeletedCount == 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Record #%d not found!</b>", targetID), smartKB(userID))
				return
			}

			recText, kb := getAdminRecordsTextAndKB(1)
			sendRawHTML(bot, chatID, txt("{E_TICK} <b>Record #%d deleted successfully!</b>\n\n", targetID)+recText, kb)
			

		case state == "adm_await_wd_time":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			startH, endH, displayStr, err := parseWithdrawTimeInput(text)
			if err != nil {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Error: %v</b>", err.Error()), smartKB(userID))
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			opts := options.Update().SetUpsert(true)
			_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$set": bson.M{
				"withdraw_start_hour": startH,
				"withdraw_end_hour":   endH,
				"withdraw_time_str":   displayStr,
			}}, opts)

			sendRawHTML(bot, chatID, txt("{E_TICK} <b>Withdraw Timing successfully updated to: %s (PKT)</b>", displayStr), smartKB(userID))

		case strings.HasPrefix(state, "await_wd_wallet_address:"):
			net := strings.TrimPrefix(state, "await_wd_wallet_address:")
			walletAddr := strings.TrimSpace(text)

			if len(walletAddr) < 10 {
				go sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid Wallet Address!</b>\nPlease enter a valid crypto address string."), nil)
				return
			}

			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			wdAcc := WithdrawAccount{
				Currency:   "usd",
				Network:    net,
				WalletAddr: walletAddr,
				IsSet:      true,
			}

			// ⚡ Direct Atomic MongoDB Write (100% Guaranteed Saving)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := usersCollection.UpdateOne(ctx, bson.M{"id": userID}, bson.M{
				"$set": bson.M{
					"wd_account": wdAcc,
					"currency":   "usd",
				},
			})
			cancel()

			if err != nil {
				log.Printf("❌ Failed to save withdraw account to Mongo: %v", err)
			}

			go sendRawHTML(bot, chatID, txt("{E_TICK} <b>Withdrawal Account Saved Successfully!</b>\n\n• <b>Currency:</b> USD\n• <b>Network:</b> %s\n• <b>Address:</b> <code>%s</code>", strings.ToUpper(net), walletAddr), smartKB(userID))


		case state == "await_wd_bank_name":
			bankName := strings.TrimSpace(text)
			if len(bankName) < 2 {
				go sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid Bank/Wallet Name!</b> Please enter a valid name."), nil)
				return
			}

			stateMu.Lock()
			userState[userID] = "await_wd_account_no:" + bankName
			stateMu.Unlock()

			go sendRawHTML(bot, chatID, txt("{E_MOBILE} <b>Please Enter Your Account Number:</b>\n\n<i>Note: Pakistani mobile numbers must be valid (e.g. 03001234567, 3001234567, or 923001234567)</i>"), nil)

		case strings.HasPrefix(state, "await_wd_account_no:"):
			bankName := strings.TrimPrefix(state, "await_wd_account_no:")
			accountNo := strings.TrimSpace(text)

			// 🚨 پاکستانی موبائل نمبرز کی سٹرکٹ چیکنگ
			if !validatePakistaniNumber(accountNo) {
				go sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid Account Number Format!</b>\n\nMust follow Pakistani number rules:\n• 11 Digits starting with <b>03</b>\n• 10 Digits starting with <b>3</b>\n• 12 Digits starting with <b>923</b>\n\nPlease enter again:"), nil)
				return
			}

			stateMu.Lock()
			userState[userID] = "await_wd_account_name:" + bankName + ":" + accountNo
			stateMu.Unlock()

			go sendRawHTML(bot, chatID, txt("{E_USERS} <b>Please Enter Correct Account Holder Name:</b>\n\n<i>Example: Muhammad Ali</i>"), nil)

		case strings.HasPrefix(state, "await_wd_account_name:"):
			parts := strings.SplitN(strings.TrimPrefix(state, "await_wd_account_name:"), ":", 2)
			bankName := parts[0]
			accountNo := parts[1]
			accountName := strings.TrimSpace(text)

			if len(accountName) < 3 {
				go sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid Account Name!</b> Please enter full name."), nil)
				return
			}

			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			wdAcc := WithdrawAccount{
				Currency:    "pkr",
				BankName:    bankName,
				AccountNo:   accountNo,
				AccountName: accountName,
				IsSet:       true,
			}

			// ⚡ Direct Atomic MongoDB Write (100% Guaranteed Saving)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := usersCollection.UpdateOne(ctx, bson.M{"id": userID}, bson.M{
				"$set": bson.M{
					"wd_account": wdAcc,
					"currency":   "pkr",
				},
			})
			cancel()

			if err != nil {
				log.Printf("❌ Failed to save withdraw account to Mongo: %v", err)
			}

			go sendRawHTML(bot, chatID, txt("{E_TICK} <b>Withdrawal Account Saved Successfully!</b>\n\n• <b>Bank/Wallet:</b> %s\n• <b>Account No:</b> <code>%s</code>\n• <b>Account Name:</b> %s", bankName, accountNo, accountName), smartKB(userID))

			

		case strings.HasPrefix(state, "ivas_await_create_file:"):
			cName := strings.TrimPrefix(state, "ivas_await_create_file:")
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			fName, count, err := saveIvasNumbersFile(cName, text)
			if err != nil || count == 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Failed to store phone numbers: %v</b>", err), smartKB(userID))
				return
			}
			sendRawHTML(bot, chatID, txt("{E_TICK} IVAS Country <b>%s</b> added successfully! Total numbers: <b>%d</b> (File: <code>%s</code>)", cName, count, fName), smartKB(userID))

		case strings.HasPrefix(state, "ivas_await_append_file:"):
			fName := strings.TrimPrefix(state, "ivas_await_append_file:")
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			total, err := appendIvasNumbersFile(fName, text)
			if err != nil {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Failed to append numbers: %v</b>", err), smartKB(userID))
				return
			}
			sendRawHTML(bot, chatID, txt("{E_TICK} Appended additional numbers to IVAS file <code>%s</code>. Total numbers: <b>%d</b>", fName, total), smartKB(userID))

		case strings.HasPrefix(state, "ivas_await_price:"):
			cName := strings.TrimPrefix(state, "ivas_await_price:")
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			priceVal, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
			if err != nil || priceVal <= 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid price rate entered. Please specify a valid number.</b>"), smartKB(userID))
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			targetOverrideMapPath := fmt.Sprintf("country_prices.%s0", strings.ToLower(cName))
			_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$set": bson.M{targetOverrideMapPath: priceVal}})
			cancel()
			go sendRawHTML(bot, chatID, txt("{E_TICK} IVAS Country rate for <b>%s</b> configured to <b>%.2f %s</b>.", cName, priceVal, CurrencySymbol), smartKB(userID))

		// ── Core Admin & User Handlers ────────────────────────────────────────
		case state == "adm_await_new_admin":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			newAdminID, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
			if err != nil || newAdminID <= 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Failed to register administrator identity: Invalid input text structure.</b>"), smartKB(userID))
				return
			}
			configMu.Lock()
			AdminIDs = append(AdminIDs, newAdminID)
			configMu.Unlock()
			syncConfigToDB()
			sendRawHTML(bot, chatID, txt("{E_TICK} <b>Administrator Profile linked and stored successfully: %d</b>", newAdminID), smartKB(userID))

		case state == "adm_await_new_api":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			cleanAPIStr := strings.TrimSpace(text)
			if cleanAPIStr == "" || !strings.HasPrefix(cleanAPIStr, "http") {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Failed to store access gateway string: Invalid URL schema parameters provided.</b>"), smartKB(userID))
				return
			}
			configMu.Lock()
			API_Bases = append(API_Bases, cleanAPIStr)
			configMu.Unlock()
			syncConfigToDB()
			sendRawHTML(bot, chatID, txt("{E_TICK} <b>API Endpoint Gateway linked and safely integrated inside core systems.</b>"), smartKB(userID))

		case state == "adm_await_active_range":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			cleanRangeName := strings.TrimSpace(text)
			if cleanRangeName == "" {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid identifier text parameters submitted.</b>"), smartKB(userID))
				return
			}
			addActiveRange(cleanRangeName)
			sendRawHTML(bot, chatID, txt("{E_TICK} <b>Active range target configured successfully: %s</b>", cleanRangeName), smartKB(userID))

		case state == "adm_await_new_app":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			parts := strings.Fields(text)
			if len(parts) == 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid service name.</b>"), smartKB(userID))
				return
			}
			name := ""
			iconID := DefaultAppIconID
			if len(parts) > 1 {
				lastPart := parts[len(parts)-1]
				if regexp.MustCompile(`^[0-9]+$`).MatchString(lastPart) {
					iconID = lastPart
					name = strings.Join(parts[:len(parts)-1], " ")
				} else {
					name = strings.Join(parts, " ")
				}
			} else {
				name = parts[0]
			}
			addApp(AppInfo{Name: name, IconID: iconID, Countries: []string{}})
			sendRawHTML(bot, chatID, txt("{E_TICK} Service <b>%s</b> added successfully.", name), smartKB(userID))

		case state == "adm_await_unpaid_app":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			serviceName := strings.TrimSpace(text)
			if serviceName == "" {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid service name.</b>"), smartKB(userID))
				return
			}
			addUnpaidService(serviceName)
			sendRawHTML(bot, chatID, txt("{E_TICK} Service <b>%s</b> marked as Unpaid successfully.", serviceName), smartKB(userID))

		case strings.HasPrefix(state, "adm_await_edit_app:"):
			appName := strings.TrimPrefix(state, "adm_await_edit_app:")
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			parts := strings.Fields(text)
			if len(parts) == 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid format.</b>"), smartKB(userID))
				return
			}
			newName := ""
			newIcon := ""
			if len(parts) > 1 {
				lastPart := parts[len(parts)-1]
				if regexp.MustCompile(`^[0-9]+$`).MatchString(lastPart) {
					newIcon = lastPart
					newName = strings.Join(parts[:len(parts)-1], " ")
				} else {
					newName = strings.Join(parts, " ")
				}
			} else {
				newName = parts[0]
			}
			updateApp(appName, newName, newIcon)
			sendRawHTML(bot, chatID, txt("{E_TICK} Service updated successfully."), smartKB(userID))

		case strings.HasPrefix(state, "adm_await_add_app_country:"):
			appName := strings.TrimPrefix(state, "adm_await_add_app_country:")
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			country := strings.TrimSpace(text)
			if country == "" {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid country identifier.</b>"), smartKB(userID))
				return
			}
			addCountryToApp(appName, country)
			sendRawHTML(bot, chatID, txt("{E_TICK} Country added to service <b>%s</b>.", appName), smartKB(userID))

		case state == "await_wd_amount":
			amount, err := strconv.ParseFloat(text, 64)
			u := getOrCreateUser(update.Message.From, 0)
			minWd := getMinWithdrawAmount()
			if err != nil || amount < minWd || amount > u.Balance {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid Cashout Balance Requirements! Request dropped.</b>"), smartKB(userID))
				return
			}
			// 🟢 اصلاح: یہاں سے بیلنس مائنس کرنے والا پاس ہٹا دیا گیا ہے۔ بیلنس اب کینسل ہونے پر ضائع نہیں ہوگا۔
			stateMu.Lock()
			userState[userID] = "await_wd_details"
			withdrawAmounts[userID] = amount
			stateMu.Unlock()
			go sendRawHTML(bot, chatID, txt("{E_GEAR} Send Transfer Payment Account Details:"), nil)

		case state == "await_wd_details":
			stateMu.Lock()
			amount := withdrawAmounts[userID]
			delete(userState, userID)
			delete(withdrawAmounts, userID)
			stateMu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// 🟢 اصلاح: بیلنس اب صرف تب اٹامک طریقے ($inc) سے کٹے گا جب یوزر اپنی اکاؤنٹ ڈیٹیلز بھیج دے گا۔
			res, err := usersCollection.UpdateOne(ctx,
				bson.M{"id": userID, "balance": bson.M{"$gte": amount}},
				bson.M{"$inc": bson.M{"balance": -amount}},
			)
			if err != nil || res.ModifiedCount == 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Insufficient balance or processing error! Request dropped.</b>"), smartKB(userID))
				return
			}

			u := getOrCreateUser(update.Message.From, 0)
			userDisplay := fmt.Sprintf("<code>%d</code>", u.ID)
			if u.Username != "" {
				userDisplay = fmt.Sprintf("@%s", u.Username)
			}
			earningsBreakdown := ""
			configMu.RLock()
			baseToFriendlyName := make(map[string]string)
			for idx, base := range API_Bases {
				baseToFriendlyName[base] = fmt.Sprintf("Panel %d (%s)", idx+1, base)
			}
			configMu.RUnlock()
			for api, amt := range u.APIEarnings {
				friendly, exists := baseToFriendlyName[api]
				if !exists {
					friendly = "Custom API Node (" + api + ")"
				}
				earningsBreakdown += fmt.Sprintf("• %s: <b>%.2f %s</b>\n", friendly, amt, CurrencySymbol)
			}
			if earningsBreakdown == "" {
				earningsBreakdown = "• No segmented API panel earnings recorded.\n"
			}
			groupText := txt("{E_GEAR} <b>New Cashout Request Pending Evaluation</b>\n\n"+
				"{E_USERS} Account Profile User: <b>%s</b>\n"+
				"{E_ADMIN} Account Full Name: <b>%s</b>\n"+
				"{E_MONEY} Cashout Amount requested: <b>%.2f %s</b>\n\n"+
				"💳 <b>Segmented Platform Earnings Breakdown:</b>\n%s\n"+
				"{E_MOBILE} <b>Account Payment Details:</b>\n<code>%s</code>",
				userDisplay, u.FirstName, amount, CurrencySymbol, earningsBreakdown, text)
			inlineAction := styledInlineKeyboardMarkup{
				InlineKeyboard: [][]styledInlineKeyboardButton{
					{
						{Text: "Approve Payment", CallbackData: fmt.Sprintf("adm_wd:approve:%d:%f", u.ID, amount), IconCustomEmojiID: ID_TICK, Style: "success"},
						{Text: "Decline Payment", CallbackData: fmt.Sprintf("adm_wd:decline:%d:%f", u.ID, amount), IconCustomEmojiID: ID_CROSS, Style: "danger"},
					},
				},
			}
			go sendRawHTML(bot, WithdrawGroupId, groupText, inlineAction)
			sendRawHTML(bot, chatID, txt("{E_TICK} <b>Request processed successfully! Balance parameters locked.</b>"), smartKB(userID))


		case state == "adm_await_price":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()
			inputArgs := strings.Fields(strings.TrimSpace(text))
			if len(inputArgs) == 0 {
				sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Action Refused: Invalid input.</b>"), smartKB(userID))
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if len(inputArgs) == 1 {
				// Global Default Price Update
				parsedVal, err := strconv.ParseFloat(inputArgs[0], 64)
				if err != nil || parsedVal <= 0 {
					sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Invalid numeric parameter.</b>"), smartKB(userID))
					return
				}
				opts := options.Update().SetUpsert(true)
				_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$set": bson.M{"otp_price": parsedVal}}, opts)
				sendRawHTML(bot, chatID, txt("{E_TICK} <b>Global default price updated to: %.2f %s</b>", parsedVal, CurrencySymbol), smartKB(userID))
			} else {
				// Specific Range, Panel, or Country Override
				priceStringVal := inputArgs[len(inputArgs)-1]
				rangeSlice := inputArgs[:len(inputArgs)-1]
				
				// "Myanmar 0-1" ya "Myanmar0-1" -> "myanmar0-1"
				rangeKey := strings.ToLower(strings.ReplaceAll(strings.Join(rangeSlice, ""), " ", ""))

				parsedVal, err := strconv.ParseFloat(priceStringVal, 64)
				if err != nil || parsedVal <= 0 || rangeKey == "" {
					sendRawHTML(bot, chatID, txt("{E_CROSS} <b>Syntax error! Format: Target Price (e.g. Myanmar0-1 1.5 or Myanmar 2)</b>"), smartKB(userID))
					return
				}
				opts := options.Update().SetUpsert(true)
				targetOverrideMapPath := fmt.Sprintf("country_prices.%s", rangeKey)
				_, _ = statsCollection.UpdateOne(ctx, bson.M{"id": "config"}, bson.M{"$set": bson.M{targetOverrideMapPath: parsedVal}}, opts)
				sendRawHTML(bot, chatID, txt("{E_TICK} <b>Price rate for '%s' configured to: %.2f %s</b>", rangeKey, parsedVal, CurrencySymbol), smartKB(userID))
			}


		case state == "adm_await_broadcast":
			stateMu.Lock()
			delete(userState, userID)
			stateMu.Unlock()

			rawInput := update.Message.Text
			rawEnts := getRawEntities(chatID, update.Message.MessageID)
			if len(rawEnts) > 0 {
				rawInput = convertEntitiesToHTML(update.Message.Text, rawEnts)
			}

			botUser, _ := bot.GetMe()
			bodyText, personalKB, groupKB := parseBroadcastPayload(botUser.UserName, rawInput)

			sendRawHTML(bot, chatID, txt("{E_MEGA} <b>Broadcasting distribution started in background!</b>"), smartKB(userID))

			go func() {
				sendRawHTML(bot, OtpGroupId, bodyText, groupKB)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
				defer cancel()

				cursor, err := usersCollection.Find(ctx, bson.M{})
				count := 0
				if err == nil {
					var allUsers []UserData
					_ = cursor.All(ctx, &allUsers)
					for _, target := range allUsers {
						time.Sleep(35 * time.Millisecond)
						go sendRawHTML(bot, target.ID, bodyText, personalKB)
						count++
					}
				}

				go sendRawHTML(bot, chatID, txt("{E_TICK} <b>Broadcast successfully finished for %d users!</b>", count), smartKB(userID))
			}()

		}
		return
	}

	// ── Normal Menu Button Action Routing ─────────────────────────────────────
	action := text
	if strings.HasPrefix(text, "/start") {
		action = "/start"
	}

	switch action {
	case "/start":
		handleStart(bot, update)
	case "Get Number":
		handleGetNumber(bot, update)
	case "My Account":
		handleMyAccount(bot, update)
	case "Stats":
		handleStats(bot, update)
	case "Withdraw":
		handleWithdraw(bot, update)
	case "Top Users":
		handleTopUsers(bot, update)
	case "Rewards":
		handleRewards(bot, update)
	case "Support":
		handleSupport(bot, update)
	case "Main Channel":
		handleMainChannel(bot, update)
	case "Admin Panel":
		if isAdmin(userID) {
			handleSupportPanel(bot, update)
		}
	}
}


// URL کے آخر میں سے پینل کا نام (np, sniper, lmx وغیرہ) نکالنے کا فنکشن
func extractApiTag(apiURL string) string {
	trimmed := strings.TrimRight(apiURL, "/")
	if idx := strings.Index(trimmed, "?"); idx != -1 {
		trimmed = trimmed[:idx]
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) > 0 {
		tag := parts[len(parts)-1]
		if tag != "" {
			return tag
		}
	}
	return "api"
}



// ✅ نیا 0ms فلی ریم کیشڈ مینیو فنکشن (Replace Code):
func countriesInlineKB(page int) styledInlineKeyboardMarkup {
	var validItems []CountryItem
	globalUsed := getGlobalUsedNumbers()

	ramNumbersMu.RLock()
	for fileName, lines := range ramNumbers {
		if strings.HasSuffix(fileName, ".txt") {
			count := 0
			for _, n := range lines {
				if n != "" && !globalUsed[n] {
					count++
				}
			}
			if count > 0 {
				validItems = append(validItems, CountryItem{
					Name:  strings.TrimSuffix(fileName, ".txt"),
					Count: count,
					File:  fileName,
				})
			}
		}
	}
	ramNumbersMu.RUnlock()

	// 🟢 A to Z الفابیٹکل ترتیب
	sort.Slice(validItems, func(i, j int) bool {
		return strings.ToLower(validItems[i].Name) < strings.ToLower(validItems[j].Name)
	})

	totalItems := len(validItems)
	pageSize := 5
	totalPages := (totalItems + pageSize - 1) / pageSize
	if totalPages == 0 { totalPages = 1 }
	if page < 1 { page = 1 }
	if page > totalPages { page = totalPages }

	var rows [][]styledInlineKeyboardButton
	if totalItems > 0 {
		start := (page - 1) * pageSize
		end := start + pageSize
		if end > totalItems { end = totalItems }
		for i := start; i < end; i++ {
			item := validItems[i]
			flagID := getFlagID(item.Name)
			rows = append(rows, []styledInlineKeyboardButton{
				{Text: fmt.Sprintf("%s (%d)", formatDisplayName(item.Name), item.Count), CallbackData: "country_select:" + item.File, IconCustomEmojiID: flagID, Style: "primary"},
			})
		}

		if totalPages > 1 {
			prevPage := page - 1
			if prevPage < 1 { prevPage = totalPages }
			nextPage := page + 1
			if nextPage > totalPages { nextPage = 1 }
			paginationControls := []styledInlineKeyboardButton{
				{Text: "Back", CallbackData: fmt.Sprintf("country_page:%d", prevPage), IconCustomEmojiID: ID_BACK, Style: "danger"},
				{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
				{Text: "Next", CallbackData: fmt.Sprintf("country_page:%d", nextPage), IconCustomEmojiID: ID_COPY, Style: "danger"},
			}
			rows = append(rows, paginationControls)
		}
	} else {
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "No Countries Available", CallbackData: "noop", IconCustomEmojiID: ID_CROSS, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Menu", CallbackData: "menu:change_country", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}


// ✅ 100% RAM-Based appCountriesInlineKB
func appCountriesInlineKB(appName string, page int) styledInlineKeyboardMarkup {
	apps := getActiveApps()
	var appCountries []string
	for _, app := range apps {
		if app.Name == appName {
			appCountries = app.Countries
			break
		}
	}

	var validItems []CountryItem
	for _, target := range appCountries {
		validNums := getValidNumbersForTarget(target, appName)
		if len(validNums) > 0 {
			validItems = append(validItems, CountryItem{
				Name:  target,
				Count: len(validNums),
				File:  target,
			})
		}
	}

	if len(validItems) == 0 {
		return styledInlineKeyboardMarkup{
			InlineKeyboard: [][]styledInlineKeyboardButton{
				{{Text: "No Countries Available", CallbackData: "noop", IconCustomEmojiID: ID_CROSS, Style: "danger"}},
				{{Text: "Back to Services", CallbackData: "menu:getnum_apps", IconCustomEmojiID: ID_BACK, Style: "danger"}},
			},
		}
	}

	pageSize := 5
	totalPages := (len(validItems) + pageSize - 1) / pageSize
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * pageSize
	end := start + pageSize
	if end > len(validItems) {
		end = len(validItems)
	}

	var rows [][]styledInlineKeyboardButton
	for i := start; i < end; i++ {
		item := validItems[i]
		flagID := getFlagID(item.Name)
		rows = append(rows, []styledInlineKeyboardButton{
			{
				Text:              fmt.Sprintf("%s (%d)", item.Name, item.Count),
				CallbackData:      "app_cselect:" + appName + ":" + item.File,
				IconCustomEmojiID: flagID,
				Style:             "primary",
			},
		})
	}

	if totalPages > 1 {
		prev := page - 1
		if prev < 1 {
			prev = totalPages
		}
		next := page + 1
		if next > totalPages {
			next = 1
		}
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "Back", CallbackData: fmt.Sprintf("appc_page:%s:%d", appName, prev), IconCustomEmojiID: ID_BACK, Style: "danger"},
			{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
			{Text: "Next", CallbackData: fmt.Sprintf("appc_page:%s:%d", appName, next), IconCustomEmojiID: ID_COPY, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Services", CallbackData: "menu:getnum_apps", IconCustomEmojiID: ID_BACK, Style: "danger"},
		{Text: "Back to Menu", CallbackData: "menu:change_country", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}



// ✅ 100% RAM-Based activeCountriesInlineKB
func activeCountriesInlineKB(page int) styledInlineKeyboardMarkup {
	activeList := getActiveRanges()
	var validItems []CountryItem

	for _, target := range activeList {
		validNums := getValidNumbersForTarget(target, "")
		if len(validNums) > 0 {
			validItems = append(validItems, CountryItem{
				Name:  target,
				Count: len(validNums),
				File:  target,
			})
		}
	}

	sort.Slice(validItems, func(i, j int) bool {
		return strings.ToLower(validItems[i].Name) < strings.ToLower(validItems[j].Name)
	})

	totalItems := len(validItems)
	pageSize := 5
	totalPages := (totalItems + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}

	var rows [][]styledInlineKeyboardButton
	if totalItems > 0 {
		start := (page - 1) * pageSize
		end := start + pageSize
		if end > totalItems {
			end = totalItems
		}
		for i := start; i < end; i++ {
			item := validItems[i]
			flagID := getFlagID(item.Name)
			rows = append(rows, []styledInlineKeyboardButton{
				{
					Text:              fmt.Sprintf("%s (%d)", formatDisplayName(item.Name), item.Count),
					CallbackData:      "country_select:" + item.File,
					IconCustomEmojiID: flagID,
					Style:             "primary",
				},
			})
		}

		if totalPages > 1 {
			prevPage := page - 1
			if prevPage < 1 {
				prevPage = totalPages
			}
			nextPage := page + 1
			if nextPage > totalPages {
				nextPage = 1
			}
			paginationControls := []styledInlineKeyboardButton{
				{Text: "Back", CallbackData: fmt.Sprintf("active_country_page:%d", prevPage), IconCustomEmojiID: ID_BACK, Style: "danger"},
				{Text: fmt.Sprintf("%d/%d", page, totalPages), CallbackData: "noop", IconCustomEmojiID: ID_TICK, Style: "success"},
				{Text: "Next", CallbackData: fmt.Sprintf("active_country_page:%d", nextPage), IconCustomEmojiID: ID_COPY, Style: "danger"},
			}
			rows = append(rows, paginationControls)
		}
	} else {
		rows = append(rows, []styledInlineKeyboardButton{
			{Text: "No Active Ranges Configured", CallbackData: "noop", IconCustomEmojiID: ID_CROSS, Style: "danger"},
		})
	}

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Back to Menu", CallbackData: "menu:change_country", IconCustomEmojiID: ID_BACK, Style: "primary"},
	})

	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}



func numbersActionKB(fileName string, nums []string, withoutCC bool) styledInlineKeyboardMarkup {
	var rows [][]styledInlineKeyboardButton

	mainCC := ""
	if len(nums) > 0 {
		cc, _ := getCountryCodeAndNational(nums[0])
		mainCC = cc
	}

	for _, n := range nums {
		clean := strings.TrimSpace(strings.TrimPrefix(n, "+"))
		displayNum := "+" + clean
		copyVal := "+" + clean

		if withoutCC {
			_, national := getCountryCodeAndNational(clean)
			displayNum = national
			copyVal = national
		}

		rows = append(rows, []styledInlineKeyboardButton{
			{
				Text:              "Copy: " + displayNum,
				IconCustomEmojiID: ID_COPY,
				Style:             "success",
				CopyText:          &copyTextObj{Text: copyVal},
			},
		})
	}

	toggleText := "Without +" + mainCC
	if withoutCC {
		toggleText = "With +" + mainCC
	}
	if mainCC == "" {
		toggleText = "Without Country Code"
		if withoutCC {
			toggleText = "With Country Code"
		}
	}

	// 🟢 صرف ۱۵-۲۰ بائٹس کا سمارٹ کال بیک (کوئی نمبر سٹرنگ نہیں بھیجی جائے گی)
	toggleCallback := fmt.Sprintf("tgcc_toggle:%s", fileName)

	rows = append(rows, []styledInlineKeyboardButton{
		{Text: toggleText, CallbackData: toggleCallback, IconCustomEmojiID: ID_PTICK, Style: "primary"},
		{Text: "Refresh Numbers", CallbackData: "country_select:" + fileName, IconCustomEmojiID: ID_TOGGLE, Style: "success"},
	})
	rows = append(rows, []styledInlineKeyboardButton{
		{Text: "Change Country", CallbackData: "menu:change_country", IconCustomEmojiID: ID_LINK, Style: "primary"},
		{Text: "Menu Closed", CallbackData: "menu:main_close", IconCustomEmojiID: ID_BACK, Style: "danger"},
	})
	return styledInlineKeyboardMarkup{InlineKeyboard: rows}
}




// ── Advanced Security & Anti-Spam Engine ──────────────────────────────────────


// Admin notification sending function
func notifyAdminsAboutSpammer(bot *tgbotapi.BotAPI, user *tgbotapi.User, totalRequests int) {
	var userLink string
	if user.UserName != "" {
		userLink = fmt.Sprintf("@%s", user.UserName)
	} else {
		userLink = fmt.Sprintf(`<a href="tg://user?id=%d">%d</a>`, user.ID, user.ID)
	}

	alertMsg := txt("{E_WARN} <b>SPAMMER AUTOMATICALLY BANNED!</b>\n\n"+
		"👤 <b>User:</b> %s\n"+
		"🆔 <b>User ID:</b> <code>%d</code>\n"+
		"⚡ <b>Requests in 10s:</b> %d\n"+
		"⏰ <b>Action:</b> Temporarily Banned for <b>1 Hours</b>\n"+
		"🚫 All future requests from this user are being dropped at server level.",
		userLink, user.ID, totalRequests)

	configMu.RLock()
	admins := make([]int64, len(AdminIDs))
	copy(admins, AdminIDs)
	configMu.RUnlock()

	for _, adminID := range admins {
		sendRawHTML(bot, adminID, alertMsg, nil)
	}
}

// ── Advanced Security & Anti-Spam Engine (UPDATED) ──────────────────────────

type UserSpamTracker struct {
	Timestamps      []time.Time
	SilentBanUntil  time.Time
	HardBannedUntil time.Time
	LastAction      string
	RepeatCount     int
}

var (
	userTrackerMap = make(map[int64]*UserSpamTracker)
	trackerMu      sync.Mutex
)

// Security Check Implementation with Behavior Analytics
func checkSecurityAndRateLimit(bot *tgbotapi.BotAPI, user *tgbotapi.User, action string) bool {
	if user == nil {
		return false
	}

	userID := user.ID
	now := time.Now()

	trackerMu.Lock()
	tracker, exists := userTrackerMap[userID]
	if !exists {
		tracker = &UserSpamTracker{
			Timestamps: []time.Time{},
		}
		userTrackerMap[userID] = tracker
	}

	// 1. ہارڈ بین چیک (1 گھنٹے کی سزا) - اس دوران مکمل اگنور مارو
	if now.Before(tracker.HardBannedUntil) {
		trackerMu.Unlock()
		return true 
	}

	// 2. سائلنٹ بین چیک (1 منٹ) اور بیہیویئر واچنگ (Behavior Watching)
	if now.Before(tracker.SilentBanUntil) {
		tracker.Timestamps = append(tracker.Timestamps, now)
		
		// واچ کریں: کیا سائلنٹ بین میں بھی پاگلوں کی طرح اسپیم کر رہا ہے؟
		spamHitsDuringBan := 0
		for _, t := range tracker.Timestamps {
			if t.After(tracker.SilentBanUntil.Add(-1 * time.Minute)) {
				spamHitsDuringBan++
			}
		}
		
		// اگر 1 منٹ کے سائلنٹ بین میں 10 بار مزید اسپیم کیا -> ٹھوک دو 1 گھنٹے کا بین!
		if spamHitsDuringBan > 10 {
			tracker.HardBannedUntil = now.Add(1 * time.Hour)
			trackerMu.Unlock()
			go notifyAdminsAboutSpammer(bot, user, spamHitsDuringBan) // اب ایڈمن کو بتاؤ
			return true
		}
		trackerMu.Unlock()
		return true // سائلنٹ ڈراپ (کوئی میسج نہیں جائے گا)
	}

	// 3. ریپیٹڈ ایکشن ڈیٹیکشن (ایک ہی بٹن بار بار دبانا)
	if action != "" && action == tracker.LastAction {
		tracker.RepeatCount++
	} else {
		tracker.LastAction = action
		tracker.RepeatCount = 1
	}

	// پچھلے 60 سیکنڈز کی ہسٹری فلٹر کریں
	var recent []time.Time
	for _, t := range tracker.Timestamps {
		if now.Sub(t) <= 60*time.Second {
			recent = append(recent, t)
		}
	}
	recent = append(recent, now)
	tracker.Timestamps = recent

	totalIn60Sec := len(recent)

	// 🛑 RULE 1: اگر ایک ہی ایکشن 7 سے زیادہ بار تیزی سے دہرایا گیا -> 1 منٹ کا سائلنٹ بین
	if tracker.RepeatCount > 7 && totalIn60Sec < 60 {
		tracker.SilentBanUntil = now.Add(1 * time.Minute)
		tracker.RepeatCount = 0 // ریسیٹ کر دیں تاکہ ہارڈ بین کی طرف جا سکے
		trackerMu.Unlock()
		return true
	}

	// 🛑 RULE 2: اگر 60 سیکنڈ میں 30 سے زیادہ ریکویسٹس ماریں -> 1 منٹ کا سائلنٹ بین
	if totalIn60Sec > 30 {
		tracker.SilentBanUntil = now.Add(1 * time.Minute)
		trackerMu.Unlock()
		return true
	}

	trackerMu.Unlock()
	return false // نارمل یوزر کو گزرنے دیں
}


// ── Per-User Dedicated Worker Queue Manager ──────────────────────────────────

type UserWorkerManager struct {
	workers map[int64]chan tgbotapi.Update
	mu      sync.Mutex
}

var UserQueue = &UserWorkerManager{
	workers: make(map[int64]chan tgbotapi.Update),
}

// Dispatch: Updates ko user ke mutabiq us ke apne worker ko bhejta hai
func (m *UserWorkerManager) Dispatch(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	var userID int64
	if update.Message != nil {
		userID = update.Message.From.ID
	} else if update.CallbackQuery != nil {
		userID = update.CallbackQuery.From.ID
	}

	// Channel ya system updates ko skip kar do
	if userID == 0 {
		return
	}

	m.mu.Lock()
	userChan, exists := m.workers[userID]
	if !exists {
		// Har user ki apni private line (Max 10 pending requests allowed per user)
		userChan = make(chan tgbotapi.Update, 10)
		m.workers[userID] = userChan
		go m.startDedicatedWorker(bot, userID, userChan)
	}
	m.mu.Unlock()

	// Request ko sirf is user ke apne channel mein dalo
	select {
	case userChan <- update:
		// Request user ki apni queue mein chali gayi
	default:
		// Agar is akelay user ki 10 requests pehle se pending hain (Flooding)
		// To is ki agli request drop ho jaye gi. Baaki kisi user par 1% bhi asar nahi parega!
	}
}

// Dedicated Worker: Sirf ek مخصوص user ki requests ko baari baari process karta hai
func (m *UserWorkerManager) startDedicatedWorker(bot *tgbotapi.BotAPI, userID int64, userChan chan tgbotapi.Update) {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	for {
		select {
		case upd, ok := <-userChan:
			if !ok {
				return
			}

			// Timer reset karo
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(30 * time.Second)

			// 1. User aur Action Text nikalen
			var user *tgbotapi.User
			var actionText string

			if upd.Message != nil {
				user = upd.Message.From
				if upd.Message.Text != "" {
					actionText = upd.Message.Text
				} else if upd.Message.Caption != "" {
					actionText = upd.Message.Caption
				}
			} else if upd.CallbackQuery != nil {
				user = upd.CallbackQuery.From
				actionText = upd.CallbackQuery.Data
			}

			// 2. Security & Rate-Limit Check (Request execution se PEHLE check karen)
			if user != nil && checkSecurityAndRateLimit(bot, user, actionText) {
				continue // Agar spammer hai to request drop kar do
			}

			// 3. Process Update (Sirf 1 Baar Execute Karega!)
			if upd.CallbackQuery != nil {
				handleCallback(bot, upd)
			} else if upd.Message != nil {
				uID := upd.Message.From.ID
				stateMu.Lock()
				state, hasState := userState[uID]
				stateMu.Unlock()

				// Agar broadcast mode active hai
				if hasState && state == "adm_await_broadcast" {
					handleBroadcastExecution(bot, upd)
				} else if upd.Message.Document != nil {
					handleDocumentMessage(bot, upd)
				} else if upd.Message.Text != "" || upd.Message.Caption != "" {
					handleTextMessage(bot, upd)
				}
			}

		case <-timer.C:
			// User 30 seconds tak inactive raha -> Garbage Collection (RAM Clean)
			m.mu.Lock()
			delete(m.workers, userID)
			close(userChan)
			m.mu.Unlock()
			return
		}
	}
}


// ── Main Pipeline ─────────────────────────────────────────────────────────────

// ── Main Pipeline ─────────────────────────────────────────────────────────────

func main() {
	time.Local = time.FixedZone("PKT", 5*60*60)
	initStorage()

	bot, err := tgbotapi.NewBotAPI(BotToken)
	if err != nil {
		log.Fatal("Bot Initialization Error: ", err)
	}

	bot.Debug = false
	startBackgroundWorkers(bot)
	log.Printf("🤖 System Execution Module Operational: @%s", bot.Self.UserName)

	offset := 0
	for {
		params := tgbotapi.Params{
			"offset":  strconv.Itoa(offset),
			"timeout": "60",
		}
		resp, err := bot.MakeRequest("getUpdates", params)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}

		// 1. افیشل ٹیلیگرام اپڈیٹس (اس سے تمام میسجز، /start اور رپلائی کی بورڈ بٹنز ۱۰۰٪ کام کریں گے)
		var updates []tgbotapi.Update
		if err := json.Unmarshal(resp.Result, &updates); err != nil {
			time.Sleep(3 * time.Second)
			continue
		}

		// 2. پریمیم ایموجیز کی رو (Raw) آئی ڈیز بغیر کسی ٹکراؤ کے محفوظ کریں
		var rawExtracts []RawUpdateExtract
		_ = json.Unmarshal(resp.Result, &rawExtracts)

		for _, rawUpd := range rawExtracts {
			if rawUpd.Message != nil && len(rawUpd.Message.Entities) > 0 {
				storeRawEntities(rawUpd.Message.Chat.ID, rawUpd.Message.MessageID, rawUpd.Message.Entities)
			}
		}

		// 3. ورکر کیو کو میسجز بھیجیں
		for _, update := range updates {
			if update.UpdateID >= offset {
				offset = update.UpdateID + 1
			}
			UserQueue.Dispatch(bot, update)
		}
	}
}


func safeToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(val, 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}


// 🟢 RAM & SQLite Sync Lock Lookup Helper with Phone Normalization
func getUserLockInfo(phoneNum string) (int64, string, bool) {
	cleanPhone := strings.TrimSpace(strings.TrimPrefix(phoneNum, "+"))
	now := time.Now()

	// 1. First check Ultra Fast RAM Cache
	ramUserLocksMu.RLock()
	for p, info := range ramUserLocks {
		pClean := strings.TrimSpace(strings.TrimPrefix(p, "+"))
		if (pClean == cleanPhone || p == phoneNum) && now.Sub(info.LockedAt) <= 10*time.Minute {
			ramUserLocksMu.RUnlock()
			return info.UserID, info.CountryFile, true
		}
	}
	ramUserLocksMu.RUnlock()

	// 2. Fallback Check in SQLite DB if not matched in RAM
	var targetUserID int64
	var countryFile string
	cutoff := now.Add(-10 * time.Minute)
	err := sqliteDB.QueryRow(`
		SELECT user_id, country_file FROM user_locks 
		WHERE (phone_number = ? OR phone_number = ? OR phone_number = ?) 
		  AND locked_at >= ? 
		ORDER BY locked_at DESC LIMIT 1`, 
		phoneNum, cleanPhone, "+"+cleanPhone, cutoff).Scan(&targetUserID, &countryFile)

	if err == nil && targetUserID > 0 {
		return targetUserID, countryFile, true
	}

	return 0, "", false
}
