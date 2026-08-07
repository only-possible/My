package main

import (
	"fmt"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// ── System Global Variables ───────────────────────────────────────────────────
var AdminIDs  = []int64{8167904992, 8291266742, 8957230182}
var API_Bases = []string{}

const (
	BotToken             = "8913014937:AAHkZtU6QmqBr6Vt3oiRUz3QuPqV71rSRjM"
	MongoURI             = "mongodb+srv://arslansalfi:786786aa@cluster0.yeycg3n.mongodb.net/?appName=Cluster0"
	DBName               = "king_earn_bot"
	WithdrawGroupId      = -1004417117659
	OtpGroupId           = -1003883753737
	CurrencySymbol       = "PKR"
	MinWithdrawAmount    = 500.0
	UseCustomEmoji       = true
	FilesDir             = "./countries_data"
	DefaultAppIconID     = "6026092115631543342"
	DefaultServiceIconID = "6026092115631543342"
	BckpChnl      = "https://whatsapp.com/channel/0029VbBUS0z1HsprNrsGSM0W"
	
	// Links
	OtpGroupInviteLink   = "https://t.me/king_all_otps"
	MainChannelLink      = "https://t.me/king_otps_channel"
	WithdrawProofsLink   = "https://t.me/+qYKsjVxtUq01Yjdk"
	DeveloperLink        = "https://t.me/only_possible0"
	AdminSupportLink     = "https://t.me/amirkingbrand"
	IvasSocketURL = "wss://ivas.tempnum.qzz.io:2087/socket.io/?token=eyJpdiI6InllZ0Q0TkI1alVPSzI3RC9oc20xZFE9PSIsInZhbHVlIjoidzdxQnRLc3ROaWJrcmZ4aXVKZ3o5ckhJNkNtQXo0N2xrT1Z0ZEZidHBjS3dhQXZIeklOQUVTdHlyUXhtYmZycWFxUDhXK2dFQk45OSsxdkZWcUJzb2l5bnpRWE45UHQyMURPZTIrYkgyN29ZcGdjV0xkNGwxSXVYcGhlVkRXN1BjbzVWbG5SYXk3QzNneGk5WFZzeDZtUVNuQnN2cXpmMzJ1dHZCekVYd1Zod2VFdWNrdk9ZNG1wN1F4STNxL28xNXY3dXZHZ0U0Q1hjaFVZYUpGbEx2Z3VGbjdySVVMMnN3YlQ4Y3IwQzgyUHV6OWROYS82UTIrc093WThpZzdsRXBCL213Wk82eCt2Z2ozWTFBeG4rLy9XdTN0blcxdWFhOUhZemw1cTZ6YkluTU9qL1dmcXFLWS9KWFdEVXN3MXdRVDN2dW9jVXk5TkVsNUltbUpjb3VBNjZnazg3Vk1tYjNqaGdIQjQ1ZTdIcjVPTk9IYnQ2S2t4aVhNbjdLdFBiVklBYlRWc0FGbGltNmxleE1uNHNSbzB6MWwweHFyY0NTOWpQcHdrTVF2eEhVRlV3NGNOa0hWZ2lVT3dQMy9rNksycEhZTEVRSi8xU0RSY2Zkc1BYd2Q0azY5QXpGNzlZZTNJcTNNbUsrWU9xRlZLRWlicXpwNVU2b05RNnRpMTlKOEVZRjcrWU5rTnIzb1BVL1Q3eWFRPT0iLCJtYWMiOiJlNzYxZThkZGY5OGEwYjMzYjViMTcwZWNkMDhjNjQ2ZmM2ODg3YzA4NjI5ZjJjMjU2ODJjZTI3OTZkZGMyMDQwIiwidGFnIjoiIn0%3D&user=d0d88a318de06e78ebcdf6d65d52cf3f&EIO=4&transport=websocket"
)

// ============================================================================
// OTP SENDER & DESIGNER FUNCTIONS (Put this in config.go)
// ============================================================================

// sendOtpToUser - یوزر کو Dynamic OTP میسج اور بٹن ڈیزائن بنا کر بھیجتا ہے
func sendOtpToUser(bot *tgbotapi.BotAPI, targetUserID int64, phoneNum, service, countryFile, otpMessage string, price float64) {
	flagID := getFlagID(strings.TrimSuffix(countryFile, ".txt"))
	serviceIcon := getServiceIcon(service)

	flagEmoji := ce(flagID, "🏳️")
	serviceEmoji := ce(serviceIcon, "📲")
	userIconEmoji := ce(ID_USER, "👤")
	moneyEmoji := ce(ID_USD, "💵")

	priceDisplay := fmt.Sprintf("%.2fPKR", price)
	maskedPhone := maskPhoneNumber(phoneNum)
	userMention := fmt.Sprintf("<a href=\"tg://user?id=%d\">%d</a>", targetUserID, targetUserID)

	// 🟢 یوزر کا ٹیکسٹ ڈیزائن
	baseText := txt("{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}\n{E_A1} %s {E_M1} %s\n{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}\n{E_A1} %s {E_M1} <code>%s</code>\n{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}\n{E_A1} %s {E_M1} %s\n{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}\n{E_A1} %s {E_M1} %s",
		userIconEmoji, userMention,
		flagEmoji, maskedPhone,
		serviceEmoji, service,
		moneyEmoji, priceDisplay)

	fullCopyText := strings.TrimSpace(otpMessage)
	if len(fullCopyText) > 256 {
		fullCopyText = fullCopyText[:256]
	}
	if fullCopyText == "" {
		fullCopyText = "000000"
	}

	// 🟢 یوزر کا ان لائن کی بورڈ
	kbUser := styledInlineKeyboardMarkup{
		InlineKeyboard: [][]styledInlineKeyboardButton{
			{
				{Text: "Copy Full MSG", CopyText: &copyTextObj{Text: fullCopyText}, IconCustomEmojiID: ID_COPY, Style: "primary"},
			},
		},
	}

	go sendRawHTML(bot, targetUserID, baseText, kbUser)
}

// sendOtpToGroup - گروپ میں Dynamic OTP میسج اور بٹن ڈیزائن بنا کر بھیجتا ہے
func sendOtpToGroup(bot *tgbotapi.BotAPI, targetUserID int64, phoneNum, service, countryFile, otpMessage, botLink string, price float64) {
	flagID := getFlagID(strings.TrimSuffix(countryFile, ".txt"))
	serviceIcon := getServiceIcon(service)

	flagEmoji := ce(flagID, "🏳️")
	serviceEmoji := ce(serviceIcon, "📲")
	userIconEmoji := ce(ID_USER, "👤")
	moneyEmoji := ce(ID_USD, "💵")

	priceDisplay := fmt.Sprintf("%.2fPKR", price)
	maskedPhone := maskPhoneNumber(phoneNum)

	var userMention string
	if targetUserID > 0 {
		userMention = fmt.Sprintf("<a href=\"tg://user?id=%d\">%d</a>", targetUserID, targetUserID)
	} else {
		userMention = "System Number"
	}

	// 🟢 گروپ کا ٹیکسٹ ڈیزائن
	baseTextGroup := txt("{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}{E_L1}\n{E_A1} %s {E_M1} %s\n{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}{E_L2}\n{E_A1} %s {E_M1} <code>%s</code>\n{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}{E_L3}\n{E_A1} %s {E_M1} %s\n{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}{E_L4}\n{E_A1} %s {E_M1} %s",
		userIconEmoji, userMention,
		flagEmoji, maskedPhone,
		serviceEmoji, service,
		moneyEmoji, priceDisplay)

	fullCopyText := strings.TrimSpace(otpMessage)
	if len(fullCopyText) > 256 {
		fullCopyText = fullCopyText[:256]
	}
	if fullCopyText == "" {
		fullCopyText = "000000"
	}

	// 🟢 گروپ کا ان لائن کی بورڈ
	kbGroup := styledInlineKeyboardMarkup{
		InlineKeyboard: [][]styledInlineKeyboardButton{
			{
				{Text: "Copy Full MSG", CopyText: &copyTextObj{Text: fullCopyText}, IconCustomEmojiID: ID_COPY, Style: "primary"},
			},
			{
				{Text: "Bot Link", URL: botLink, IconCustomEmojiID: ID_LINK, Style: "success"},
			},
			{
				{Text: "Main Channel", URL: MainChannelLink, IconCustomEmojiID: ID_CHNL, Style: "danger"},
			},
		},
	}

	queueGroupMessage(OtpGroupId, baseTextGroup, kbGroup)
}
