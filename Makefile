.PHONY: build run test clean docker-build docker-run lint fmt deps

# Binary name
BINARY_NAME=sms-gateway
BUILD_DIR=./bin

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOLINT=golangci-lint

# Build flags
LDFLAGS=-ldflags "-s -w"

# Default target
all: deps build

# Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Build the binary
build:
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/gateway

# Build for Linux (for Docker)
build-linux:
	mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux ./cmd/gateway

# Run the application
run:
	$(GOCMD) run ./cmd/gateway

# Run with hot reload (requires air: go install github.com/cosmtrek/air@latest)
dev:
	air

# Run tests
test:
	$(GOTEST) -v -race ./...

# Run tests with coverage
test-coverage:
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Run integration tests
test-integration:
	$(GOTEST) -v -tags=integration ./tests/integration/...

# Lint the code
lint:
	$(GOLINT) run ./...

# Format the code
fmt:
	$(GOFMT) -s -w .

# Clean build artifacts
clean:
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Docker build
docker-build:
	docker build -t emalify-sms-gateway:latest .

# Docker run
docker-run:
	docker-compose up -d

# Docker stop
docker-stop:
	docker-compose down

# Docker logs
docker-logs:
	docker-compose logs -f sms-gateway

# Generate mocks (requires mockgen)
mocks:
	mockgen -source=internal/sms/ports/mno_sender.go -destination=internal/sms/ports/mocks/mno_sender_mock.go -package=mocks
	mockgen -source=internal/sms/ports/queue_consumer.go -destination=internal/sms/ports/mocks/queue_consumer_mock.go -package=mocks
	mockgen -source=internal/sms/ports/queue_publisher.go -destination=internal/sms/ports/mocks/queue_publisher_mock.go -package=mocks
	mockgen -source=internal/sms/ports/token_cache.go -destination=internal/sms/ports/mocks/token_cache_mock.go -package=mocks

# Show help
help:
	@echo "Available targets:"
	@echo "  make deps          - Download dependencies"
	@echo "  make build         - Build the binary"
	@echo "  make build-linux   - Build for Linux"
	@echo "  make run           - Run the application"
	@echo "  make dev           - Run with hot reload"
	@echo "  make test          - Run tests"
	@echo "  make test-coverage - Run tests with coverage"
	@echo "  make lint          - Lint the code"
	@echo "  make fmt           - Format the code"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make docker-build  - Build Docker image"
	@echo "  make docker-run    - Run with Docker Compose"
	@echo "  make docker-stop   - Stop Docker Compose"
	@echo "  make mocks         - Generate mock files"