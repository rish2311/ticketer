# ── Build Stage ──
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Copy shared module first (for caching)
COPY shared/ /app/shared/

# Copy order-worker module
COPY services/order-worker/ /app/services/order-worker/

WORKDIR /app/services/order-worker

# Download dependencies
RUN go mod download

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /order-worker .

# ── Runtime Stage ──
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /order-worker .

EXPOSE 9091

CMD ["./order-worker"]
