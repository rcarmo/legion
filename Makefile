.PHONY: build test lint check clean fmt docs

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
	RUST_LOG=legion=debug,hiqlite=info cargo run -p legion-server -- \
		--data-dir /tmp/legion-dev \
		--listen 0.0.0.0:7777

# Run tests for a specific crate
test-core:
	cargo test -p legion-core

test-store:
	cargo test -p legion-store

test-loop:
	cargo test -p legion-loop
