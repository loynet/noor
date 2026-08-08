BINARY ?= noor
IMAGE_TAG ?= $(shell git rev-parse --short HEAD 2>/dev/null || printf local)
IMAGE ?= noor:$(IMAGE_TAG)
NOOR_ENV ?= dev

ifeq ($(NOOR_ENV),dev)
ENV_FILE ?= .env.dev
CONFIG_FILE ?= config/dev.toml
CONTAINER ?= noor-dev
VOLUME ?= noor-dev-data
else ifeq ($(NOOR_ENV),prod)
ENV_FILE ?= .env.prod
CONFIG_FILE ?= config/prod.toml
CONTAINER ?= noor
VOLUME ?= noor-data
else
$(error NOOR_ENV must be dev or prod)
endif
DOCKER_RUN_EXTRA ?=

.PHONY: fmt test lint check build run docker-build docker-check-config docker-run docker-deploy docker-logs docker-clean

fmt:
	gofmt -w cmd internal

test:
	GOCACHE="$(PWD)/.gocache" go test ./...

lint:
	GOCACHE="$(PWD)/.gocache" go vet ./...

check: fmt lint test

build:
	GOCACHE="$(PWD)/.gocache" go build -trimpath -buildvcs=false -o $(BINARY) ./cmd/noor

run:
	set -a; . ./$(ENV_FILE); set +a; CONFIG_FILE=$(CONFIG_FILE) go run ./cmd/noor

docker-build:
	docker build -t $(IMAGE) .

docker-check-config:
	docker run --rm --env-file $(ENV_FILE) \
		-e CONFIG_FILE=/etc/noor/config.toml \
		--mount type=bind,source=$(abspath $(CONFIG_FILE)),target=/etc/noor/config.toml,readonly \
		--read-only --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m \
		--cap-drop ALL --security-opt no-new-privileges \
		$(IMAGE) check-config

docker-run:
	docker run -d --name $(CONTAINER) --restart unless-stopped \
		--env-file $(ENV_FILE) -e CONFIG_FILE=/etc/noor/config.toml \
		--mount type=bind,source=$(abspath $(CONFIG_FILE)),target=/etc/noor/config.toml,readonly \
		--mount type=volume,source=$(VOLUME),target=/app/data \
		--read-only --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m \
		--cap-drop ALL --security-opt no-new-privileges \
		$(DOCKER_RUN_EXTRA) $(IMAGE)

docker-deploy: docker-build docker-check-config

	-docker rm -f $(CONTAINER)
	$(MAKE) docker-run

docker-logs:
	docker logs -f $(CONTAINER)

docker-clean:
	-docker rm -f $(CONTAINER)
	-docker volume rm $(VOLUME)
