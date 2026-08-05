package main

import (
	"fmt"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// ── System Global Variables ───────────────────────────────────────────────────

var AdminIDs = []int64{8167904992, 8752000921}
var API_Bases = []string{} // خالی بریکٹ، اب پینل سے ایڈ کی گئی APIs ہی لوڈ ہوں گی

// ── Global System Configuration Constants ─────────────────────────────────────

const (
	// Core Credentials & Database Connections
	BotToken      = "8882146140:AAFTR2M9qcPYcRc0rMDvR59QCrOzGgDi89Q"
	MongoURI      = "mongodb+srv://arslansalfi:786786aa@cluster0.yeycg3n.mongodb.net/?appName=Cluster0"
	DBName        = "otp_earn_bot"
	IvasSocketURL = "wss://ivas.tempnum.qzz.io:2087/socket.io/?token=eyJpdiI6IllvRG56V21CNy9xS3dQbUYvMHgrYkE9PSIsInZhbHVlIjoiQVlrZDFzOHJpb3F3S0U2Tzh0Y3d2K3llSmJJT1pWMkgxNXFJR2x1WTZzR2p3Zmg4S2JmSHdrZWQ0WThzcHVzMGE1RDl3c0RRaEpqM0dyZU0vaUtTRVNOVWFSa3ZsRzJpazVzdjEyVExDenFZWEx1Wkp4elRKNSs3OFRHM29mL2piY3IwMmVwYU84UnhIdUt0dThDSU9HdzNQUWFSc0FpckJrOVFZakszdm5JUWRId1h5ZXdpejlSc2g5ZFM3VktwelRSMk15bkVYQTRxZ3YrVExCcng5ZUtwUTR4bFZMbEV6UTdFT2NxVXlBMkRzaVVZYlBYRWF4TXg5UTQrQ1lseTdWSXhQaDc2Ni9zVC9yTkNhWGZ1THJOUHViZDZMb1JINkFLUTQrVno2T3FMRi9qVXZYKzFkcGM0TnJoMUJBdDJ6b3Z1NUZoVERRQi9FRUJXSVhxRlFvYmJNYkZDVU0zTUVmSUZENG5oUy8yVWM2WVVIckNVZnZ0NFdzaDIzVzRBRklPR1NuVHFNL29MUituc2pteEtjWFpYaE84aXZRbEtrZVllWHJZVldFQkZDaFd6TmtSTmRyVDFQZW1GQno4UlB4VUNEc1ZtNlBTcUFKU1J5OEg5eDZMRityNThaYmJrYzRjNGZwWG82Qng0a3dMY0RweDNFUEdXejlMc3FaeE0rQm1HUGZtRlhMRDIwT1ovaE42UUwrTGt6ZVdrVzU1RzNXWVdkZ0gwRzc0PSIsIm1hYyI6ImE5NDJhZmQ5ZThiMDI3ZDY1ZDk5YzQwZjY4MjJiNWIzYzdmZjdmYjc0YTczYmQyMWU3MDQ3YjU3MzY5ODk3YTUiLCJ0YWciOiIifQ%3D%3D&user=2a47d3f43fd5e745e72eaee137d95228&EIO=4&transport=websocket"

	// Telegram Groups & General Settings
	WithdrawGroupId   = -1003856692661
	OtpGroupId        = -1003393993086
	CurrencySymbol    = "PKR"
	MinWithdrawAmount = 100.0
	UseCustomEmoji    = true
	FilesDir          = "./countries_data"

	// Default Emoji IDs
	DefaultAppIconID     = "6026092115631543342"
	DefaultServiceIconID = "6026092115631543342"

	// Official System Links
	OtpGroupInviteLink = "https://t.me/total_otps"
	MainChannelLink    = "https://t.me/imp_tst"
	WithdrawProofsLink = "https://t.me/+xwUa2q2y1QI1NGY0"
	DeveloperLink      = "https://t.me/only_possible0"
	AdminSupportLink   = "https://t.me/Awaisrazakhan"
	BckpChnl           = "https://whatsapp.com/channel/0029VbBUS0z1HsprNrsGSM0W"
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
	baseText := txt("<blockquote expandable>{E_A1} %s {E_M1} %s\n{E_A1} %s {E_M1} %s\n{E_A1} %s {E_M1} %s\n{E_A1} %s {E_M1} %s</blockquote>",
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
		userMention = "8167904992"
	}

	// 🟢 گروپ کا ٹیکسٹ ڈیزائن
	baseTextGroup := txt("<blockquote expandable>{E_A1} %s {E_M1} %s\n{E_A1} %s {E_M1} %s\n{E_A1} %s {E_M1} %s\n{E_A1} %s {E_M1} %s</blockquote>",
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
