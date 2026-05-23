# syntax=docker/dockerfile:1.7

# ---- build ----
FROM golang:1.26-alpine AS build

WORKDIR /src

# Cache modules first.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Build a static binary.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
        -o /out/sender ./cmd/server

# ---- runtime ----
FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget tini \
    && adduser -D -H -u 10001 app

COPY --from=build /out/sender /usr/local/bin/sender

USER app
ENV ADDR=:8080
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/usr/local/bin/sender"]
