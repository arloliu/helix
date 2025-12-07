# Makefile for helix project
# Usage: make [target]

# Configuration
TEST_TIMEOUT    ?= 6m
LINT_TIMEOUT    ?= 3m
COVERAGE_DIR    := ./.coverage
COVERAGE_OUT    := $(COVERAGE_DIR)/coverage.out
COVERAGE_HTML   := $(COVERAGE_DIR)/coverage.html

# Source files
ALL_GO_FILES    := $(shell find . -name "*.go" -not -path "./vendor/*")
TEST_DIRS       := $(sort $(dir $(shell find . -name "*_test.go" -not -path "./vendor/*" -not -path "./test/integration/*")))
INTEGRATION_DIR := ./test/integration/...
LATEST_GIT_TAG  := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")

# Linter configuration
LINTER_GOMOD          := -modfile=linter.go.mod
GOLANGCI_LINT_VERSION := 2.5.0

# Default target
.DEFAULT_GOAL := help

.PHONY: help test test-unit test-integration test-all test-quick clean-test-results
.PHONY: coverage coverage-html
.PHONY: lint linter-update linter-version fmt vet
.PHONY: generate gomod-tidy clean ci

## help: Show this help message
help:
	@echo "Available targets:" && \
	grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

##@ Testing

## test: Run all tests with race detector
test: test-all

## test-unit: Run only unit tests (same as test)
test-unit: clean-test-results
	@echo "Running unit tests..."
	@CGO_ENABLED=1 go test $(TEST_DIRS) -count=1 -timeout=$(TEST_TIMEOUT) -race

## test-integration: Run only integration tests
test-integration: clean-test-results
	@echo "Running integration tests..."
	@CGO_ENABLED=1 go test $(INTEGRATION_DIR) -count=1 -v -timeout=$(TEST_TIMEOUT) -race

## test-all: Run unit and integration tests
test-all: clean-test-results
	@echo "Running all tests (unit + integration)..."
	@echo "==> Running unit tests..."
	@CGO_ENABLED=1 go test $(TEST_DIRS) -count=1 -timeout=$(TEST_TIMEOUT) -race
	@echo "==> Running integration tests..."
	@CGO_ENABLED=1 go test $(INTEGRATION_DIR) -count=1 -v -timeout=$(TEST_TIMEOUT) -race
	@echo "All tests passed!"

## test-quick: Run unit tests without race detection (faster)
test-quick: clean-test-results
	@echo "Running unit tests without race detection..."
	@CGO_ENABLED=0 go test $(TEST_DIRS) -count=1 -short -timeout=$(TEST_TIMEOUT)

## coverage: Generate test coverage report (unit packages only)
coverage: clean-test-results
	@mkdir -p $(COVERAGE_DIR)
	@echo "Generating coverage report..."
	@go test $(TEST_DIRS) -coverprofile=$(COVERAGE_OUT) -covermode=atomic -timeout=$(TEST_TIMEOUT)
	@go tool cover -func=$(COVERAGE_OUT) | tail -1

## coverage-html: Generate HTML coverage report and open in browser
coverage-html: coverage
	@echo "Generating HTML coverage report..."
	@go tool cover -html=$(COVERAGE_OUT) -o $(COVERAGE_HTML)
	@echo "Coverage report generated: $(COVERAGE_HTML)"

## clean-test-results: Clean test artifacts
clean-test-results:
	@rm -f test.log *.pprof
	@rm -rf $(COVERAGE_DIR)
	@go clean -testcache

##@ Code Quality

## linter-update: Install/update golangci-lint tool
linter-update:
	@echo "Install/update linter tool..."
	@go get -tool $(LINTER_GOMOD) github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$(GOLANGCI_LINT_VERSION)
	@go mod verify $(LINTER_GOMOD)

## linter-version: Show installed golangci-lint version
linter-version:
	@go tool $(LINTER_GOMOD) golangci-lint --version

## lint: Run linters
lint:
	@echo "Checking golangci-lint version..."
	@INSTALLED_VERSION=$$(go tool $(LINTER_GOMOD) golangci-lint --version 2>/dev/null | grep -oE 'version [^ ]+' | cut -d' ' -f2 || echo "not-installed"); \
	if [ "$$INSTALLED_VERSION" = "not-installed" ]; then \
		echo "Error: golangci-lint not found. Run 'make linter-update' to install."; \
		exit 1; \
	elif [ "$$INSTALLED_VERSION" != "$(GOLANGCI_LINT_VERSION)" ]; then \
		echo "Warning: golangci-lint version mismatch!"; \
		echo "  Expected: $(GOLANGCI_LINT_VERSION)"; \
		echo "  Installed: $$INSTALLED_VERSION"; \
		echo "  Run 'make linter-update' to install the correct version."; \
		exit 1; \
	else \
		echo "✓ golangci-lint $(GOLANGCI_LINT_VERSION) is installed"; \
	fi
	@echo "Running linters..."
	@go tool $(LINTER_GOMOD) golangci-lint run --timeout=$(LINT_TIMEOUT)

## fmt: Format code
fmt:
	@echo "Formatting code..."
	@gofmt -s -w .
	@if command -v goimports >/dev/null 2>&1; then \
		goimports -w $(ALL_GO_FILES); \
	else \
		echo "Note: goimports not installed. Run 'go install golang.org/x/tools/cmd/goimports@latest' for import formatting."; \
	fi

## vet: Run go vet
vet:
	@echo "Running go vet..."
	@go vet ./...

##@ Build & Dependencies

## generate: Run go generate for code generation (msgp, etc.)
generate:
	@echo "Running go generate..."
	@go generate ./...

## gomod-tidy: Tidy go.mod and go.sum
gomod-tidy:
	@echo "Tidying go modules..."
	@go mod tidy
	@go mod verify

##@ Cleanup

## clean: Clean all build artifacts and caches
clean: clean-test-results
	@echo "Cleaning build artifacts..."
	@go clean -cache
	@rm -rf dist/ bin/

##@ CI/CD

## ci: Run all CI checks (lint, vet, test, coverage)
ci: lint vet test-all coverage

.PHONY: update-pkg-cache
update-pkg-cache:
	@echo "Updating package cache with latest git tag: $(LATEST_GIT_TAG)"
	@curl -sf https://proxy.golang.org/github.com/arloliu/helix/@v/$(LATEST_GIT_TAG).info > /dev/null || \
		echo "Warning: Failed to update package cache"
