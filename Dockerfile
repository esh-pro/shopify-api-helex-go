# Helex Shopify API — Render (512MB free tier)
FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go addrbook.go ./
RUN CGO_ENABLED=0 go build -o shopapi .

FROM alpine:3.20
WORKDIR /app
COPY --from=build /app/shopapi ./shopapi
# 512MB free tier: small caps, lean GC, capped CPU threads.
# (Fleet of N instances; bot balances across Mongo API URLs.)
ENV GOGC=20
ENV GOMAXPROCS=2
ENV PER_USER_CONCURRENT=30
# PORT is injected by Render; code honors $PORT (default 5001).
EXPOSE 5001
CMD ["./shopapi"]
