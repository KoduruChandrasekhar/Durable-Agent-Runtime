# Builds keel-server, keel (CLI), keel-gateway and keel-sandbox.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY gen ./gen
COPY internal ./internal
COPY server ./server
COPY gateway ./gateway
COPY sandbox ./sandbox
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/keel-server ./server/cmd/keel-server && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/keel ./server/cmd/keel && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/keel-gateway ./gateway/cmd/keel-gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/keel-sandbox ./sandbox/cmd/keel-sandbox

FROM alpine:3.20 AS server
RUN adduser -D -u 10001 keel && mkdir -p /data/blobs && chown keel /data/blobs
COPY --from=build /out/keel-server /out/keel /usr/local/bin/
COPY deploy/policy.yaml /etc/keel/policy.yaml
USER keel
ENV KEEL_POLICY_FILE=/etc/keel/policy.yaml KEEL_BLOB_DIR=/data/blobs
EXPOSE 8080 7233
ENTRYPOINT ["keel-server"]

FROM alpine:3.20 AS gateway
RUN adduser -D -u 10001 keel
COPY --from=build /out/keel-gateway /usr/local/bin/
COPY deploy/gateway.yaml /etc/keel/gateway.yaml
USER keel
ENV KEEL_GATEWAY_CONFIG=/etc/keel/gateway.yaml
EXPOSE 8090
ENTRYPOINT ["keel-gateway"]

# The sandbox drives Docker through the socket (effectively root on the host):
# run it against rootless Docker or on a separate VM.
FROM docker:27-cli AS sandbox
COPY --from=build /out/keel-sandbox /usr/local/bin/
EXPOSE 8095
ENTRYPOINT ["keel-sandbox"]
