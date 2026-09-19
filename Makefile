# Legal Tender Go rewrite build and verification targets.

include scripts/go-build.env
GO_IMAGE := $(LT_GO_IMAGE)
GO_CACHE_DIR := $(HOME)/.cache/legal-tender
GO_DOCKER_ENV := -e GOCACHE=/gocache -e GOMODCACHE=/gomodcache -e GOTOOLCHAIN=local -e GOMEMLIMIT=2GiB -e GOMAXPROCS=4
GO_DOCKER_CACHES := -v $(GO_CACHE_DIR)/gocache:/gocache -v $(GO_CACHE_DIR)/gomodcache:/gomodcache
GO_DOCKER_VOLUMES := -v $(PWD):/src $(GO_DOCKER_CACHES)
GO_DOCKER_LIMITS := --memory=4g --memory-swap=4g --cpus=4
GO_RUN := docker run --rm $(GO_DOCKER_LIMITS) $(GO_DOCKER_ENV) $(GO_DOCKER_VOLUMES) -w /src $(GO_IMAGE)

.PHONY: help cache-init build release-deps release-build test test-race test-window-integration fmt fmt-check vet tidy tidy-check check

help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

cache-init: ## Create persistent Go compiler and module caches.
	@mkdir -p $(GO_CACHE_DIR)/gocache $(GO_CACHE_DIR)/gomodcache

build: cache-init ## Compile every Go command and package.
	$(GO_RUN) go build -buildvcs=false ./...

release-build: ## Retain and compare clean offline release builds; requires BUILD_OUTPUT=new-directory.
	bash scripts/release-go.sh "$(BUILD_OUTPUT)"

release-deps: cache-init ## Fetch pinned modules into the local cache; network allowed, source read-only.
	docker run --rm $(GO_DOCKER_LIMITS) $(GO_DOCKER_ENV) -v $(PWD):/src:ro $(GO_DOCKER_CACHES) -w /src $(GO_IMAGE) bash scripts/build-go.sh dependencies

test: cache-init ## Run the complete Go test suite.
	$(GO_RUN) go test -buildvcs=false ./...

test-race: cache-init ## Run the Go suite with the race detector.
	$(GO_RUN) go test -buildvcs=false -race ./...

test-window-integration: ## Test the full two-publication loader in disposable ArangoDB; retain logs and remove test volumes.
	LT_GO_CACHE_DIR=$(GO_CACHE_DIR) bash scripts/test-window-integration.sh

fmt: cache-init ## Format all Go source files.
	$(GO_RUN) gofmt -w -s cmd internal

fmt-check: cache-init ## Fail when Go source needs formatting.
	@unformatted="$$( $(GO_RUN) gofmt -l cmd internal )"; \
	if [ -n "$$unformatted" ]; then \
		printf 'gofmt required:\n%s\n' "$$unformatted"; \
		exit 1; \
	fi

vet: cache-init ## Run Go static analysis.
	$(GO_RUN) go vet ./...

tidy: cache-init ## Update Go module metadata.
	$(GO_RUN) go mod tidy

tidy-check: cache-init ## Fail when module metadata is not canonical.
	$(GO_RUN) go mod tidy -diff

check: fmt-check tidy-check vet test ## Run every non-destructive Go gate.
