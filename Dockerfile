FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH
RUN arch="${TARGETARCH:-$(go env GOARCH)}" \
	&& CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$arch" \
	go build -mod=readonly -trimpath -buildvcs=false -ldflags="-s -w" -o /out/noor ./cmd/noor \
	&& mkdir -p /out/app/data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/noor /usr/local/bin/noor
COPY --from=build --chown=10001:10001 /out/app /app
USER 10001:10001
WORKDIR /app
ENV HEALTHCHECK_ADDR=127.0.0.1:9090
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD ["/usr/local/bin/noor", "check-health"]
ENTRYPOINT ["/usr/local/bin/noor"]
