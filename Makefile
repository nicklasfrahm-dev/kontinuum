# Colors
CYAN := \033[36m
GREEN := \033[32m
BOLD := \033[1m
RESET := \033[0m

# Binary
BINDIR := bin
BINARY := $(BINDIR)/kontinuum

# Install
INSTALLDIR ?= $(HOME)/.local/bin

# Version, derived from git. Falls back to "dev" outside a git repo.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-s -w -X github.com/nicklasfrahm/kontinuum/pkg/cli.version=$(VERSION)"

# Go commands
GOCMD := go
# -trimpath and the linker's -s -w (stripped symbol table/DWARF) apply to
# every build target — local, air, and the Containerfile, which all now
# build through this same target — for a smaller binary that doesn't embed
# this machine's absolute source paths.
GOBUILD := $(GOCMD) build -trimpath $(LDFLAGS)
GOTEST := $(GOCMD) test
GOMOD := $(GOCMD) mod

.DEFAULT_GOAL := help

##@ General

.PHONY: help
help: ## Display this help
	@printf '\n'
	@printf '$(BOLD)Usage:$(RESET)\n'
	@printf '  $(CYAN)make$(RESET) <target>\n'
	@printf '\n'
	@awk 'BEGIN {FS = ":.*##"; printf ""} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  $(CYAN)%-15s$(RESET) %s\n", $$1, $$2 } /^##@/ { printf "\n$(BOLD)%s$(RESET)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@printf '\n'

##@ Development

.PHONY: generate
# GOOS/GOARCH forced empty (native), regardless of what's exported in the
# calling environment: every go:generate directive here (see
# api/v1alpha2/doc.go and pkg/ui/assets.go) shells out to `go tool`/`go run`,
# which both build their tool and immediately execute it, so if a
# cross-compiling caller (see build's GOOS/GOARCH, set for the Containerfile's
# multi-arch image build) left those exported, either would try to run a
# foreign-architecture binary and fail with "exec format error".
generate: ## Regenerate deepcopy methods, CRDs, and vendored web assets
	GOOS= GOARCH= go generate ./...

.PHONY: build
build: generate ## Build the binary
	@mkdir -p $(BINDIR)
	$(GOBUILD) -o $(BINARY) ./cmd/kontinuum

.PHONY: run
run: build ## Run the server locally with dev-friendly logging (info, console)
	KONTINUUM_LOG_LEVEL=info KONTINUUM_LOG_FORMAT=console ./$(BINARY) serve

.PHONY: install
install: build ## Build the binary and install it to ~/.local/bin
	@mkdir -p $(INSTALLDIR)
	install $(BINARY) $(INSTALLDIR)/kontinuum

.PHONY: dev
dev: ## Start development environment with hot reload (air + postgres + proxy)
	@printf '$(CYAN)Starting development environment...$(RESET)\n'
	docker compose --profile dev up

.PHONY: dev-down
dev-down: ## Stop development environment
	docker compose --profile dev down

.PHONY: dev-clean
dev-clean: ## Stop development environment and remove volumes
	@printf '$(CYAN)Cleaning development environment volumes...$(RESET)\n'
	docker compose --profile dev down -v

.PHONY: image
image: ## Build the container image
	docker buildx build -f Containerfile -t kontinuum:$(VERSION) --load .

##@ Quality

.PHONY: test
test: generate ## Run tests
	$(GOTEST) -v ./...

.PHONY: test-e2e
# TestE2E* is this repo's naming convention (see
# pkg/domain/instance/talos_e2e_test.go's own doc) for gated tests that need
# Docker and boot real containers — selected by name here, and by
# .github/workflows/ci.yml's "E2E" job, rather than run as part of the
# default `test` target above. The 15m timeout accounts for
# pkg/domain/taloscluster's own TestE2E test, which bootstraps a real Talos
# control plane and worker and installs Cilium/cert-manager for real —
# considerably slower than instance's own maintenance-mode-only e2e test.
test-e2e: generate ## Run gated end-to-end tests (requires Docker; boots real containers)
	KONTINUUM_TEST_E2E=1 $(GOTEST) -v ./... -run '^TestE2E' -timeout 15m

.PHONY: vet
vet: generate ## Run go vet
	$(GOCMD) vet ./...

.PHONY: lint
lint: generate ## Run golangci-lint
	go tool golangci-lint run

.PHONY: lint-fix
lint-fix: generate ## Run golangci-lint and fix issues
	go tool golangci-lint run --fix

.PHONY: tidy
tidy: ## Download and tidy dependencies
	$(GOMOD) download
	$(GOMOD) tidy

##@ Documentation

DOCSVENV := .venv/docs

.PHONY: docs
docs: ## Serve documentation locally with live-reload (http://127.0.0.1:8000)
	@printf '$(CYAN)Installing documentation dependencies...$(RESET)\n'
	@python3 -m venv $(DOCSVENV)
	@$(DOCSVENV)/bin/pip install --quiet -r docs/requirements.txt
	$(DOCSVENV)/bin/mkdocs serve

##@ Cleanup

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BINDIR) pkg/ui/assets/vendor
	$(GOCMD) clean

.PHONY: docs-clean
docs-clean: ## Remove the documentation virtualenv and built site
	rm -rf $(DOCSVENV) site
