# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:alpine AS builder

WORKDIR /app

# Copy module file and auto-generate correct go.sum
#COPY go.mod ./
RUN go mod tidy

# Copy source and build
COPY . .
RUN go build -o nexabot .

# ── Stage 2: Run ──────────────────────────────────────────────────────────────
FROM alpine:latest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/nexabot .

# data.json will persist here


CMD ["./nexabot"]
