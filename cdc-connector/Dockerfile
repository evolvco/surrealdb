# Build stage
FROM golang:1.25-alpine AS builder

# Git is needed for go mod to fetch source-only pseudo-version deps.
RUN apk add --no-cache git

WORKDIR /src

# Cache deps separately from source for faster rebuilds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary: CGO off, stripped symbols.
# GOMAXPROCS=1 during build limits the compiler's memory footprint; the
# TiDB-transitively dependency graph is large enough to OOM the default
# Docker build sandbox otherwise.
ENV CGO_ENABLED=0
ENV GOMAXPROCS=1
RUN go build -ldflags="-s -w" -p 1 -o /out/cdc-connector ./cmd/cdc-connector

# Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/cdc-connector /cdc-connector

USER nonroot:nonroot
ENTRYPOINT ["/cdc-connector"]
