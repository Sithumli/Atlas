.PHONY: build test test-race lint fmt clean up down docs

BIN_DIR := bin
BINARIES := atlas-ctrler atlas-kv atlas-cli

build:
	@mkdir -p $(BIN_DIR)
	@for bin in $(BINARIES); do \
		echo "building $$bin"; \
		go build -o $(BIN_DIR)/$$bin ./cmd/$$bin; \
	done

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

clean:
	rm -rf $(BIN_DIR)
	rm -rf data/ snapshots/

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

docs:
	cd docs && latexmk -pdf Atlas.tex
