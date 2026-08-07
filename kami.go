package main

import (
	"fmt"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)
// ── System Global Variables ───────────────────────────────────────────────────

var AdminIDs = []int64{8167904992, 6022286935}
var API_Bases = []string{} // خالی بریکٹ، اب پینل سے ایڈ کی گئی APIs ہی لوڈ ہوں گی

// ── Global System Configuration Constants ─────────────────────────────────────

const (
	// Core Credentials & Database Connections
	BotToken      = "8982209312:AAEnKeK2b5dh0cffqhQf3Lg4tzzabW6P9SA"
	MongoURI      = "mongodb+srv://arslansalfi:786786aa@cluster0.yeycg3n.mongodb.net/?appName=Cluster0"
	DBName        = "kami_otp_bot"
	IvasSocketURL = "wss://ivas.tempnum.qzz.io:2087/socket.io/?token=eyJpdiI6Iis5emtldEV0NHpuWWdaTlRFNUgzMWc9PSIsInZhbHVlIjoicnBDSUxpRWVySk9BZ0ZHaGs5RWhRei8xWCtoaVpIM1VWWU15ZGF4ZGlKVlo3MmtHcUI1aFhSRllmU0hLQ3ZuZlkzaVRsOE1qc2I5R3orL2ZDMGt3TGFzNTNoMXg5R1E2RGtIZ0VGQ0lSUGVmQmxVUHJURS9GckkzQTA3dVF1L3ZyUEc0d2dJNzhyaFQzM1Y2OGlQbTA4WDR1M0RIa0lFNFVQaFArZndPWDNsSU83R1NTck9VYTBGTEFvMDlobk1KMmRQazVlV1phSi9McnpaSldpQ05vVU95SDNqNTl3U0pIMGIzS1ZlY1pqV3ViSVJsUjlhVEdubVl3Vk5Ud0plWkM0a2ZneEduMzBreGE4S2VQQmFuRUtreTRMVlVyN3NMZ0E3TkFtTFFzRlEvQ2FERFVaMmRUNE1scER0RnR5UmVJTUJyQWdYM2MwS3NicFpXUXBkNk82MGpXdEw1OXhINHE4T05IblN4VnoyUlpYWHdhbVRLaVVtTmx6NGYvNE03ZWJJRG5GbXdSNmFBeHkrSVZVc2ZDTFJDYnVFdnZMNEVucS9NeUI3dVVQRW8rNzdZSFMyZUhiamFpKzdBbU9tVUJLYWNpMmtkNlhtcHI3RDI3SkxqaEVyZUhZS1p4UjVnSjEwcm5Hd0lYaisxTjlzbi9MRlNSUWNBS0E1aG5qOGhDcEFxN2pJMm5ON1J5Z3ZxQ1hZVS96b1FRQkEvTTVCUHRDZlcyZ2Vqb2ZFPSIsIm1hYyI6ImY3ZmZmYzg1ZmY2OGZiMTk5MDRmNWZlZDA4Mzk0ODE0MGM4Mjk5Y2NmM2Y0ZGU3OTdlZDE4ZDc0NDczZWJiMDkiLCJ0YWciOiIifQ%3D%3D&user=2873ef831039f8fc401603317d3bc4bc&EIO=4&transport=websocket"


	// Telegram Groups & General Settings
	WithdrawGroupId   = -1003839692384
	OtpGroupId        = -1003534182528
	CurrencySymbol    = "PKR"
	MinWithdrawAmount = 100.0
	UseCustomEmoji    = true
	FilesDir          = "./countries_data"

	// Default Emoji IDs
	DefaultAppIconID     = "6026092115631543342"
	DefaultServiceIconID = "6026092115631543342"

	// Official System Links
	OtpGroupInviteLink = "https://t.me/kami_total_otps"
	MainChannelLink    = "https://t.me/kami_otp_earning"
	WithdrawProofsLink = "https://t.me/+Jmog1I6SMdRkZTVk"
	DeveloperLink      = "https://t.me/only_possible0"
	AdminSupportLink   = "https://t.me/Mr_Kaamii"
	BckpChnl      = "https://whatsapp.com/channel/0029VbC7FKh1iUxTMBdLXb2L"
)


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
