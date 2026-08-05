# Stage 1: Build Stage (Go کا لیٹسٹ/ڈائنامک Alpine ورژن خودکار اٹھے گا)
FROM golang:alpine AS builder

# SQLite (go-sqlite3) کی CGO کمپائلیشن کے لیے ضروری ٹولز
RUN apk add --no-cache gcc musl-dev ca-certificates tzdata

WORKDIR /app

# تمام سورس فائلز کو کنٹینر میں کاپی کریں
COPY . .

# اگر go.mod موجود نہ ہو تو خود بنائے گا اور تمام External Dependencies ڈاؤن لوڈ کرے گا
RUN if [ ! -f go.mod ]; then go mod init main_app; fi && \
    go mod tidy

# Binary کمپائل کریں (CGO ان ایبل کے ساتھ SQLite کے لیے)
ENV CGO_ENABLED=1
RUN go build -ldflags="-s -w" -o bot .

# Stage 2: Final Lightweight Runtime Image
FROM alpine:latest

# ضروری رن ٹائم لائبریریز (SSL Certificates اور Timezone)
RUN apk add --no-cache ca-certificates tzdata sqlite-libs

WORKDIR /app

# Builder اسٹیج سے تیار شدہ Binary کاپی کریں
COPY --from=builder /app/bot /app/bot
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# ٹائم زون سیٹ کریں (PKT / Asia/Karachi)
ENV TZ=Asia/Karachi

# بوٹ چلانے کی کمانڈ
CMD ["/app/bot"]
