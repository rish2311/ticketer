# ── Build Stage ──
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Copy shared module first (for caching)
COPY shared/ /app/shared/

# Copy api-gateway module
COPY services/api-gateway/ /app/services/api-gateway/

WORKDIR /app/services/api-gateway

# Download dependencies
RUN go mod download

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /api-gateway .

# ── Runtime Stage ──
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /api-gateway .

EXPOSE 8080

CMD ["./api-gateway"]
