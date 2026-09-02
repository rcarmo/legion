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

.PHONY: go-fmt go-fmt-check go-test go-test-core go-test-store go-test-agent go-vet go-check go-build go-rust-interop go-ninep-interop go-blob-interop \
	go-otel-integration-test go-process-smoke go-backup-restore-drill go-load-test go-load-test-raft go-load-test-http go-systemd-package-test go-verify-m4 go-install \
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
	  '  make go-verify-m4        One-pass Go Milestone 4 verification' \
	  '  make go-test-core        Focused Go core contract tests' \
	  '  make go-test-store       Focused pure-Go SQLite tests'

go-fmt:
	$(GO_ENV) $(GO) fmt ./cmd/... ./internal/...

go-fmt-check:
	@test -z "$$(gofmt -l cmd internal 2>/dev/null)" || { gofmt -l cmd internal; exit 1; }

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

go-build: preflight
	@mkdir -p $(CURDIR)/bin
	$(GO_ENV) $(GO) build -trimpath -o $(CURDIR)/bin/legion ./cmd/legion


go-rust-interop: preflight
	$(CARGO) build -p legion-cluster --bin legion-interop-fixture --bin legion-gossip-interop-fixture
	$(GO_ENV) $(GO) test -tags rustinterop -count=1 -timeout=120s -run 'TestRustGo(Direct|Relay|Gossip)' -v ./internal/cluster

go-ninep-interop: preflight
	$(CARGO) build -p legion-cluster --bin legion-ninep-interop-fixture
	$(GO_ENV) $(GO) test -tags rustinterop -count=1 -timeout=60s -run TestRustGoNinePInterop -v ./internal/namespace

go-blob-interop: preflight
	CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) $(CARGO) build -p legion-cluster --bin legion-blob-interop-fixture
	$(GO_ENV) $(GO) test -tags rustinterop -count=1 -timeout=60s -run 'Test(RustServesGo|GoServesRust)FetchesBlob' -v ./internal/deploy

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
	CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) $(CARGO) build --manifest-path tests/fixtures/wasm-hello/Cargo.toml --release --target wasm32-wasip1
	CARGO_TARGET_DIR=$(CARGO_TARGET_DIR) $(CARGO) build --manifest-path tests/fixtures/wasm-host/Cargo.toml --release --target wasm32-wasip1

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

go-install: go-build
	BINARY=$(CURDIR)/bin/legion ./contrib/systemd/install.sh

uninstall:
	./contrib/systemd/uninstall.sh

JOKER_VERSION ?= v1.8.0
JOKER_REVISION := edd0fe7fff7b2bae3a714a9918502f7dd3b21d5f
JOKER_BIN ?= $(CURDIR)/bin/joker

.PHONY: joker-worker go-verify-m3
joker-worker:
	@mkdir -p "$(dir $(JOKER_BIN))"
	@dir="$$(GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO) mod download -json github.com/rcarmo/go-joker@$(JOKER_VERSION) | awk -F'"' '/"Dir":/ {print $$4}')"; \
	 test -n "$$dir" || { echo 'ERROR: cannot download pinned Joker'; exit 1; }; \
	 cd "$$dir" && GOTOOLCHAIN=$(GO_TOOLCHAIN) CGO_ENABLED=0 $(GO) build -trimpath -ldflags '-X github.com/candid82/joker/core.VERSION=$(JOKER_VERSION)' -o "$(JOKER_BIN)" .

# Pure-Go Milestone 3 gate, including the separately built Joker worker and the
# existing Extism fixture shared with the Rust implementation.
go-verify-m3: preflight joker-worker wasm-fixture go-blob-interop
	LEGION_JOKER_TEST_BIN="$(JOKER_BIN)" LEGION_BUN_TEST_BIN="$$(command -v bun)" $(GO_ENV) $(GO) test -count=1 ./internal/deploy ./internal/runtime/...
	LEGION_JOKER_TEST_BIN="$(JOKER_BIN)" LEGION_BUN_TEST_BIN="$$(command -v bun)" $(GO_ENV) $(GO) test -count=1 ./internal/namespace ./cmd/legion
	$(GO_ENV) $(GO) build -o $(CURDIR)/bin/legion ./cmd/legion
	LEGION_BUN_BIN="$$(command -v bun)" ./tests/integration/go_m3_smoke.sh
	@$(MAKE) --no-print-directory postflight

go-otel-integration-test: preflight
	$(GO_ENV) $(GO) test -tags oteltest -count=1 -run '^TestAgentLifecycleAndTokenUsageReachOTLP$$' -v ./internal/telemetry

go-process-smoke: go-build
	$(GO_ENV) $(GO) build -trimpath -o $(CURDIR)/bin/ninep-smoke ./cmd/ninep-smoke
	LEGION_BUN_BIN="$$(command -v bun)" ./tests/integration/go_m4_process_smoke.sh

go-backup-restore-drill: go-build
	@command -v restic >/dev/null || { echo 'ERROR: restic is required' >&2; exit 1; }
	LEGION_BUN_BIN="$$(command -v bun)" ./tests/integration/restic_restore_drill.sh

go-load-test-raft: preflight
	$(GO_ENV) $(GO) test -tags loadtest -count=1 -run '^TestThreeNodeReplicatedBatchLoad$$' -v ./internal/raftstore

go-load-test-http: go-build
	LEGION_BUN_BIN="$$(command -v bun)" ./tests/load/run_http.sh

go-load-test: go-load-test-raft go-load-test-http

go-systemd-package-test: go-build
	@stage="$$(mktemp -d)"; trap 'rm -rf "$$stage"' EXIT; \
	 DESTDIR="$$stage" BINARY=$(CURDIR)/bin/legion ./contrib/systemd/install.sh; \
	 test -x "$$stage/usr/local/bin/legion"; \
	 test -f "$$stage/etc/systemd/system/legion.service"; \
	 test -f "$$stage/etc/legion/legion.env"; \
	 ! grep -q 'LEGION_CONFIG\| serve$$' "$$stage/etc/systemd/system/legion.service"; \
	 systemd-analyze verify "$$stage/etc/systemd/system/legion.service"

go-verify-m4: preflight go-fmt-check
	@git diff --check
	$(GO_ENV) $(GO) vet ./...
	$(GO_ENV) $(GO) test -count=1 ./...
	$(MAKE) --no-print-directory go-rust-interop
	$(MAKE) --no-print-directory go-ninep-interop
	$(MAKE) --no-print-directory go-otel-integration-test
	$(MAKE) --no-print-directory go-process-smoke
	$(MAKE) --no-print-directory go-backup-restore-drill
	$(MAKE) --no-print-directory go-systemd-package-test
	$(MAKE) --no-print-directory go-load-test
	@$(MAKE) --no-print-directory postflight
