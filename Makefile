.PHONY: build test lint check clean fmt docs integration-test

build:
	cargo build --workspace

release:
	cargo build --workspace --release

test:
	cargo test --workspace

lint:
	cargo clippy --workspace --all-targets -- -D warnings

fmt:
	cargo fmt --all

check:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace

clean:
	cargo clean

docs:
	cargo doc --workspace --no-deps --open

# Run a single-node server in dev mode
dev:
	RUST_LOG=legion=debug,hiqlite=info \
	cargo run -p legion-server

# Run tests for a specific crate
test-core:
	cargo test -p legion-core

test-store:
	cargo test -p legion-store

test-loop:
	cargo test -p legion-loop

test-namespace:
	cargo test -p legion-namespace

test-deploy:
	cargo test -p legion-deploy

test-cluster:
	cargo test -p legion-cluster -- --test-threads=1

# Integration test: deploy a Bun function and invoke it directly via the REST API.
# Requires: bun installed, LEGION_TEST_PORT available.
integration-test: build
	@echo "==> Legion integration test"
	@./tests/integration/run.sh

# Build the server binary only
server:
	cargo build -p legion-server
