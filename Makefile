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
LDFLAGS := -ldflags "-X github.com/nicklasfrahm/kontinuum/pkg/cli.version=$(VERSION)"

# Go commands
GOCMD := go
GOBUILD := $(GOCMD) build $(LDFLAGS)
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
generate: ## Regenerate deepcopy methods and CRDs for api/... via controller-gen
	go tool controller-gen object paths="./api/..."
	go tool controller-gen crd paths="./api/..." output:crd:artifacts:config=config/crd

.PHONY: build
build: ## Build the binary
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
dev: ## Start development environment with hot reload (air + postgres)
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
test: ## Run tests
	$(GOTEST) -v ./...

.PHONY: vet
vet: ## Run go vet
	$(GOCMD) vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	go tool golangci-lint run

.PHONY: lint-fix
lint-fix: ## Run golangci-lint and fix issues
	go tool golangci-lint run --fix

.PHONY: tidy
tidy: ## Download and tidy dependencies
	$(GOMOD) download
	$(GOMOD) tidy

##@ Cleanup

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BINDIR)
	$(GOCMD) clean
