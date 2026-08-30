.PHONY: build test lint check clean fmt docs integration-test cli-integration-test wasm-integration-test install uninstall

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

cli-integration-test: server
	@echo "==> Legion CLI integration test"
	@./tests/integration/cli.sh

wasm-integration-test: server
	@echo "==> Legion WASM integration test"
	@cd tests/fixtures/wasm-hello && cargo build --release --target wasm32-wasip1
	@PORT=$${LEGION_TEST_PORT:-18090}; DATA=$$(mktemp -d); LOG=$$(mktemp); \
	 LEGION_API_PORT=$$PORT LEGION_DATA_DIR=$$DATA RUST_LOG=error ./target/debug/legion serve >$$LOG 2>&1 & PID=$$!; \
	 trap 'kill $$PID 2>/dev/null || true; rm -rf $$DATA $$LOG' EXIT; \
	 for _ in $$(seq 1 50); do curl -sf http://127.0.0.1:$$PORT/health >/dev/null && break; sleep .2; done; \
	 ./tests/integration/wasm_smoke.sh $$PORT

# Build the server binary only
server:
	cargo build -p legion-server

# Stage with DESTDIR=/tmp/pkg; set ENABLE=1 for a live systemd installation.
install:
	BINARY=target/release/legion ./contrib/systemd/install.sh

uninstall:
	./contrib/systemd/uninstall.sh
