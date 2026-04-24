# syntax=docker/dockerfile:1.6

# ---- builder ----
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/atlas-ctrler ./cmd/atlas-ctrler \
 && CGO_ENABLED=0 go build -o /out/atlas-kv      ./cmd/atlas-kv      \
 && CGO_ENABLED=0 go build -o /out/atlas-cli     ./cmd/atlas-cli

# ---- ctrler ----
FROM alpine:3.19 AS ctrler
RUN mkdir -p /var/lib/atlas
COPY --from=builder /out/atlas-ctrler /usr/local/bin/atlas-ctrler
EXPOSE 9200 9201
ENTRYPOINT ["atlas-ctrler"]

# ---- kv ----
FROM alpine:3.19 AS kv
RUN mkdir -p /var/lib/atlas
COPY --from=builder /out/atlas-kv /usr/local/bin/atlas-kv
EXPOSE 9300 9301
ENTRYPOINT ["atlas-kv"]

# ---- cli ----
FROM alpine:3.19 AS cli
COPY --from=builder /out/atlas-cli /usr/local/bin/atlas-cli
ENTRYPOINT ["atlas-cli"]
