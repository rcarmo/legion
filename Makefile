SHELL := /bin/bash
.DEFAULT_GOAL := help

# Keep every Cargo invocation—including fixture builds—in one reusable tree.
export CARGO_TARGET_DIR := $(CURDIR)/target
CARGO ?= $(HOME)/.cargo/bin/cargo
MIN_FREE_GB ?= 6
TARGET_WARN_GB ?= 10
GO ?= go
GO_TOOLCHAIN ?= go1.26.5
GO_ENV := GOTOOLCHAIN=$(GO_TOOLCHAIN) CGO_ENABLED=0

.PHONY: go-fmt go-fmt-check go-test go-test-core go-test-store go-test-agent go-vet go-check \
	help preflight postflight space build release test lint fmt fmt-check check verify-m3 \
	clean clean-junk distclean docs dev server integration-test cli-integration-test \
	wasm-fixture wasm-integration-test otel-integration-test backup-restore-drill bun-ninep-integration-test dashboard-integration-test js-test load-test load-test-http load-test-hiqlite server-release install uninstall test-core test-store test-loop \
	test-namespace test-deploy test-cluster test-ecosystem test-runtime-extism

help:
	@printf '%s\n' \
	  'Legion build entry points (all use ./target):' \
	  '  make verify-m3           One-pass Milestone 3 verification' \
	  '  make check               Format, lint, and test the workspace' \
	  '  make integration-test    Build once, then run Bun integration tests' \
	  '  make wasm-integration-test  Build server/fixture once, then smoke test' \
	  '  make clean-junk          Remove temp files and accidental nested targets' \
	  '  make clean               Remove Cargo build output' \
	  '  make space               Show free space and build-tree size' \
	  '  make go-check            Pure-Go format, vet, and test gate (CGO disabled)' \
	  '  make go-test-core        Focused Go core contract tests' \
	  '  make go-test-store       Focused pure-Go SQLite tests'

go-fmt:
	$(GO_ENV) $(GO) fmt ./internal/...

go-fmt-check:
	@test -z "$$(gofmt -l internal 2>/dev/null)" || { gofmt -l internal; exit 1; }

go-test-core: preflight
	$(GO_ENV) $(GO) test ./internal/core

go-test-store: preflight
	$(GO_ENV) $(GO) test ./internal/store

go-test-agent: preflight
	$(GO_ENV) $(GO) test ./internal/agent

go-test: preflight
	$(GO_ENV) $(GO) test ./...

go-vet: preflight
	$(GO_ENV) $(GO) vet ./...

go-check: preflight go-fmt-check
	$(GO_ENV) $(GO) vet ./...
	$(GO_ENV) $(GO) test ./...

preflight: clean-junk
	@free_gb=$$(df -Pk "$(CURDIR)" | awk 'NR==2 {print int($$4/1024/1024)}'); \
	 if (( free_gb < $(MIN_FREE_GB) )); then \
	   echo "ERROR: only $${free_gb} GiB free; need $(MIN_FREE_GB) GiB before building" >&2; exit 1; \
	 fi

postflight:
	@$(MAKE) --no-print-directory clean-junk
	@size_gb=$$(du -sk "$(CARGO_TARGET_DIR)" 2>/dev/null | awk '{print int($$1/1024/1024)}'); \
	 if (( size_gb >= $(TARGET_WARN_GB) )); then \
	   echo "WARNING: target is $${size_gb} GiB (threshold $(TARGET_WARN_GB) GiB); run 'make clean' when finished" >&2; \
	 fi

space:
	@df -h "$(CURDIR)"
	@du -sh "$(CARGO_TARGET_DIR)" 2>/dev/null || true

build: preflight
	$(CARGO) build --workspace
	@$(MAKE) --no-print-directory postflight

release: preflight
	$(CARGO) build --workspace --release
	@$(MAKE) --no-print-directory postflight

test: preflight
	$(CARGO) test --workspace
	@$(MAKE) --no-print-directory postflight

lint: preflight
	$(CARGO) clippy --workspace --all-targets -- -D warnings
	@$(MAKE) --no-print-directory postflight

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

# Sequential by design: never launch overlapping Cargo builds on this VM.
check: preflight
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --workspace --all-targets -- -D warnings
	$(CARGO) test --workspace
	@$(MAKE) --no-print-directory postflight

# Milestone 3's narrow gate. Cargo reuses the same artifacts for the server and
# Extism-enabled runtime tests; integration scripts do not invoke Cargo again.
verify-m3: preflight
	@git diff --check
	$(CARGO) test -p legion-runtime --features extism --no-fail-fast
	$(CARGO) build -p legion-server
	./tests/integration/run.sh
	$(MAKE) --no-print-directory wasm-fixture
	./tests/integration/wasm_server_smoke.sh
	@$(MAKE) --no-print-directory postflight

clean:
	$(CARGO) clean
	@$(MAKE) --no-print-directory clean-junk

# Safe, repository-local housekeeping. Do not delete the shared target here: it
# is the cache that prevents repeated full builds.
clean-junk:
	@find "$(CURDIR)" -mindepth 2 -type d -name target ! -path "$(CARGO_TARGET_DIR)" -prune -exec rm -rf {} + 2>/dev/null || true
	@find "$(CURDIR)" -type f \( -name '*.tmp' -o -name '*.temp' -o -name 'core' -o -name 'core.*' \) -delete 2>/dev/null || true

# Explicit full reset, including Cargo output and repository-local junk.
distclean: clean

# Build the server binary only.
server: preflight
	$(CARGO) build -p legion-server
	@$(MAKE) --no-print-directory postflight

server-release: preflight
	$(CARGO) build -p legion-server --release
	@$(MAKE) --no-print-directory postflight

# Run a single-node server in dev mode.
dev: preflight
	RUST_LOG=legion=debug,hiqlite=info $(CARGO) run -p legion-server

# Targeted tests.
test-core test-store test-loop test-namespace test-deploy test-cluster test-ecosystem test-runtime-extism: preflight
	@case "$@" in \
	 test-core) package=legion-core;; test-store) package=legion-store;; \
	 test-loop) package=legion-loop;; test-namespace) package=legion-namespace;; \
	 test-deploy) package=legion-deploy;; test-cluster) package=legion-cluster;; \
	 test-ecosystem) package=legion-ecosystem;; \
	 test-runtime-extism) package='legion-runtime --features extism';; esac; \
	 threads=''; [[ "$@" == test-cluster ]] && threads='-- --test-threads=1'; \
	 $(CARGO) test -p $$package $$threads
	@$(MAKE) --no-print-directory postflight

integration-test: server
	@echo "==> Legion integration test"
	@./tests/integration/run.sh

cli-integration-test: server
	@echo "==> Legion CLI integration test"
	@./tests/integration/cli.sh

wasm-fixture: preflight
	$(CARGO) build --manifest-path tests/fixtures/wasm-hello/Cargo.toml --release --target wasm32-wasip1

wasm-integration-test: server wasm-fixture
	@./tests/integration/wasm_server_smoke.sh
	@$(MAKE) --no-print-directory postflight

otel-integration-test: preflight
	$(CARGO) build -p legion-server --bin otel-probe
	@./tests/integration/otel_collector_smoke.sh
	@$(MAKE) --no-print-directory postflight

js-test:
	bun install --frozen-lockfile
	bun run build:client
	bun run typecheck
	bun -e 'import("./packages/client/dist/index.js").then(m => { if (typeof m.LegionClient !== "function") process.exit(1) })'
	bun test packages

bun-ninep-integration-test: server js-test
	@./tests/integration/bun_ninep_smoke.sh
	@$(MAKE) --no-print-directory postflight

dashboard-integration-test: server js-test
	@./tests/integration/dashboard_smoke.sh
	@$(MAKE) --no-print-directory postflight

backup-restore-drill: clean-junk
	@command -v restic >/dev/null || { echo 'ERROR: restic is required' >&2; exit 1; }
	@./tests/integration/restic_restore_drill.sh

load-test: load-test-hiqlite load-test-http

load-test-hiqlite: preflight
	$(CARGO) test -p legion-store --features distributed --test hiqlite_load --release -- --ignored --nocapture --test-threads=1
	@$(MAKE) --no-print-directory postflight

load-test-http: server-release
	@./tests/load/run_http.sh
	@$(MAKE) --no-print-directory postflight

docs: preflight
	$(CARGO) doc --workspace --no-deps --open

# Stage with DESTDIR=/tmp/pkg; set ENABLE=1 for a live systemd installation.
install:
	BINARY=$(CARGO_TARGET_DIR)/release/legion ./contrib/systemd/install.sh

uninstall:
	./contrib/systemd/uninstall.sh
