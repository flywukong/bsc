# State Performance Testing Tool Makefile

.PHONY: build clean run test help install deps

BINARY_NAME=pert-tool
VERSION=1.0.0
BUILD_DIR=.
SOURCE_DIR=.

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Build flags
LDFLAGS=-ldflags "-X main.version=$(VERSION)"

# Default target
all: build

# Build the binary
build:
	@echo "Building $(BINARY_NAME) v$(VERSION)..."
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) $(SOURCE_DIR)
	@echo "Build complete: $(BINARY_NAME)"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf bench-pebble
	rm -rf test-bench-pebble
	@echo "Clean complete"

# Install dependencies
deps:
	@echo "Installing dependencies..."
	$(GOMOD) tidy
	$(GOMOD) download
	@echo "Dependencies installed"

# Run basic performance test
run: build
	@echo "Running basic performance test..."
	./$(BINARY_NAME) press-test

# Run with custom read/write ratios
run-custom: build
	@echo "Running custom performance test..."
	./$(BINARY_NAME) press-test --read-ratio 0.7 --write-ratio 0.2 --update-ratio 0.1

# Run high write intensity test
run-write-heavy: build
	@echo "Running write-heavy performance test..."
	./$(BINARY_NAME) press-test --read-ratio 0.2 --write-ratio 0.6 --update-ratio 0.2 --batch 8000

# Run long duration test
run-long: build
	@echo "Running long duration test..."
	./$(BINARY_NAME) press-test --runtime 5m --batch 10000

# Run test with specific test case
run-testcase: build
	@echo "Running test with specific test case..."
	./$(BINARY_NAME) press-test --testcase test-case --batch 5000 --runtime 2m

# Run with sharding database
run-sharding: build
	@echo "Running test with sharding database..."
	./$(BINARY_NAME) press-test --sharding-db --runtime 3m --batch 5000

# Install the binary to GOPATH/bin
install: build
	@echo "Installing $(BINARY_NAME) to $(GOPATH)/bin..."
	cp $(BINARY_NAME) $(GOPATH)/bin/
	@echo "Installation complete"

# Show help for available commands
show-help: build
	@echo "Available commands for $(BINARY_NAME):"
	./$(BINARY_NAME) --help
	@echo ""
	./$(BINARY_NAME) press-test --help

# Run development test
dev-test: build
	@echo "Running development test..."
	./$(BINARY_NAME) press-test --batch 1000 --runtime 30s --read-ratio 0.5 --write-ratio 0.3 --update-ratio 0.2

# Benchmark with different batch sizes
benchmark-batch: build
	@echo "=== Testing Batch Size 1000 ==="
	./$(BINARY_NAME) press-test --batch 1000 --runtime 1m
	@echo ""
	@echo "=== Testing Batch Size 5000 ==="
	./$(BINARY_NAME) press-test --batch 5000 --runtime 1m
	@echo ""
	@echo "=== Testing Batch Size 10000 ==="
	./$(BINARY_NAME) press-test --batch 10000 --runtime 1m

# Benchmark with different ratios
benchmark-ratios: build
	@echo "=== Testing Read-heavy (80% read, 10% write, 10% update) ==="
	./$(BINARY_NAME) press-test --read-ratio 0.8 --write-ratio 0.1 --update-ratio 0.1 --runtime 1m
	@echo ""
	@echo "=== Testing Write-heavy (20% read, 60% write, 20% update) ==="
	./$(BINARY_NAME) press-test --read-ratio 0.2 --write-ratio 0.6 --update-ratio 0.2 --runtime 1m
	@echo ""
	@echo "=== Testing Update-heavy (30% read, 20% write, 50% update) ==="
	./$(BINARY_NAME) press-test --read-ratio 0.3 --write-ratio 0.2 --update-ratio 0.5 --runtime 1m

# Performance stress test
stress-test: build
	@echo "Running stress test with high concurrency..."
	./$(BINARY_NAME) press-test --threads 8 --batch 10000 --runtime 10m --sharding-db

# Quick test for development
quick: build
	@echo "Running quick test..."
	./$(BINARY_NAME) press-test --runtime 10s --batch 500 --warmup 5s

# Show binary info
info: build
	@echo "Binary: $(BINARY_NAME)"
	@echo "Version: $(VERSION)"
	@echo "Size: $$(du -h $(BINARY_NAME) | cut -f1)"
	@echo "Build date: $$(date)"
	./$(BINARY_NAME) --version

# Help target
help:
	@echo "Available targets:"
	@echo "  build          - Build the $(BINARY_NAME) binary"
	@echo "  clean          - Remove build artifacts"
	@echo "  deps           - Install Go dependencies"
	@echo "  run            - Run basic performance test"
	@echo "  run-custom     - Run with custom ratios"
	@echo "  run-write-heavy- Run write-heavy test"
	@echo "  run-long       - Run long duration test"
	@echo "  run-testcase   - Run with specific test case"
	@echo "  run-sharding   - Run with sharding database"
	@echo "  install        - Install binary to GOPATH/bin"
	@echo "  show-help      - Show application help"
	@echo "  dev-test       - Run development test"
	@echo "  benchmark-batch- Benchmark different batch sizes"
	@echo "  benchmark-ratios- Benchmark different operation ratios"
	@echo "  stress-test    - Run high-load stress test"
	@echo "  quick          - Quick test for development"
	@echo "  info           - Show binary information"
	@echo "  help           - Show this help message"
	@echo ""
	@echo "Example usage:"
	@echo "  make build"
	@echo "  make run"
	@echo "  make run-sharding"
	@echo "  make stress-test"
