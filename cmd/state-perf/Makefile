# State Performance Testing Tool Makefile

.PHONY: build clean run test help install deps

BINARY_NAME=state-perf
VERSION=1.0.0

# Build the binary
build:
	go build -ldflags="-X main.version=$(VERSION)" -o $(BINARY_NAME) .

# Clean build artifacts
clean:
	rm -f $(BINARY_NAME)
	rm -rf bench-pebble
	rm -rf test-bench-pebble

# Install dependencies
deps:
	go mod tidy
	go mod download

# Run basic performance test
run: build
	./$(BINARY_NAME) press-test

# Run with custom read/write ratios
run-custom: build
	./$(BINARY_NAME) press-test --read-ratio 0.7 --write-ratio 0.2 --update-ratio 0.1

# Run high write intensity test
run-write-heavy: build
	./$(BINARY_NAME) press-test --read-ratio 0.2 --write-ratio 0.6 --update-ratio 0.2 --batch 8000

# Run long duration test
run-long: build
	./$(BINARY_NAME) press-test --runtime 5m --batch 10000

# Run test with specific test case
run-testcase: build
	./$(BINARY_NAME) press-test --testcase test-case --batch 5000 --runtime 2m

# Install the binary to GOPATH/bin
install: build
	cp $(BINARY_NAME) $(GOPATH)/bin/

# Show help for available commands
show-help: build
	./$(BINARY_NAME) --help
	@echo ""
	./$(BINARY_NAME) press-test --help

# Run development test
dev-test: build
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
	@echo "=== Testing Read Heavy (80/10/10) ==="
	./$(BINARY_NAME) press-test --read-ratio 0.8 --write-ratio 0.1 --update-ratio 0.1 --runtime 1m
	@echo ""
	@echo "=== Testing Balanced (60/20/20) ==="
	./$(BINARY_NAME) press-test --read-ratio 0.6 --write-ratio 0.2 --update-ratio 0.2 --runtime 1m
	@echo ""
	@echo "=== Testing Write Heavy (20/60/20) ==="
	./$(BINARY_NAME) press-test --read-ratio 0.2 --write-ratio 0.6 --update-ratio 0.2 --runtime 1m

# Show help
help:
	@echo "Available targets:"
	@echo "  build          - Build the state-perf binary"
	@echo "  clean          - Remove build artifacts and benchmark databases"
	@echo "  deps           - Install/update dependencies"
	@echo "  run            - Build and run basic performance test"
	@echo "  run-custom     - Build and run with custom ratios (70/20/10)"
	@echo "  run-write-heavy- Build and run write-heavy test (20/60/20)"
	@echo "  run-long       - Build and run 5-minute test with large batch"
	@echo "  run-testcase   - Build and run with specific test case"
	@echo "  install        - Install binary to GOPATH/bin"
	@echo "  show-help      - Show command line help"
	@echo "  dev-test       - Run quick development test"
	@echo "  benchmark-batch- Compare different batch sizes"
	@echo "  benchmark-ratios- Compare different read/write ratios"
	@echo "  help           - Show this help message"

# Default target
default: build 