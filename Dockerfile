FROM golang:1.26.3-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

ADD go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /build/busybee ./cmd

# --------------------
FROM golang:1.26.3-alpine AS goose_builder
WORKDIR /build

RUN apk add --no-cache git
RUN git clone https://github.com/pressly/goose .
RUN go mod tidy
RUN go build \
    -tags='no_sqlite3 no_clickhouse no_mssql no_mysql no_turso no_vertica no_ydb' \
    -o goose ./cmd/goose

# --------------------
FROM scratch AS final

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=builder /build/busybee /busybee
COPY --from=goose_builder /build/goose /goose

COPY ./migrations /migrations

EXPOSE 9842

# Run the binary directly as the entrypoint
ENTRYPOINT ["/busybee"]