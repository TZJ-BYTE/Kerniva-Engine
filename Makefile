.PHONY: all build build-server build-client run client clean test race fmt deps help

all: build

build:
	bash scripts/build.sh

build-server build-client: build

run: build
	./bin/kerniva-server

client: build
	./bin/kerniva-client

clean:
	bash scripts/clean.sh

test:
	bash scripts/test.sh

race:
	bash scripts/test.sh -race

fmt:
	go fmt ./cmd/... ./config/... ./internal/... ./pkg/... ./tests/...
	cargo fmt --manifest-path kerniva_engine/Cargo.toml

deps:
	go mod download

help:
	@echo "make build | run | client | clean | test | race | fmt"
