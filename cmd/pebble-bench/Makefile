# PebbleDB Benchmark Tool Makefile

.PHONY: build clean run test help

# Build the binary
build:
	go build -o pebble-bench .

# Clean build artifacts
clean:
	rm -f pebble-bench
	rm -rf bench-pebble

# Run with default parameters
run: build
	./pebble-bench

# Run with custom batch size
run-batch: build
	./pebble-bench -batch 10000

# Run benchmark test
test: build
	./pebble-bench -testcase test-case -batch 5000 -bench-db test-bench-pebble

# Install dependencies
deps:
	go mod tidy

# Show help
help:
	@echo "Available targets:"
	@echo "  build      - Build the pebble-bench binary"
	@echo "  clean      - Remove build artifacts and benchmark database"
	@echo "  run        - Build and run with default parameters"
	@echo "  run-batch  - Build and run with batch size 10000"
	@echo "  test       - Build and run benchmark test"
	@echo "  deps       - Install/update dependencies"
	@echo "  help       - Show this help message"

# Default target
default: build 