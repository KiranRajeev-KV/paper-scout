FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/paper-scout ./cmd/server

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/paper-scout /usr/local/bin/paper-scout
COPY config/default.yaml config/default.yaml
RUN mkdir -p /app/logs && chown -R app:app /app
USER app
EXPOSE 8080
ENTRYPOINT ["paper-scout"]
