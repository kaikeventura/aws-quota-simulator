FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /quota-simulator ./cmd/server/main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /quota-simulator .
COPY configs/ configs/

EXPOSE 4567

ENV CONFIG_PATH=/app/configs/quotas.yaml

CMD ["./quota-simulator"]