# Makefile for random_data_loader Go project

.PHONY: help build test lint lint-fix tidy clean run env-up env-down

## Show help for each make target
help:
	@echo "Usage: make <target>"
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

## Build the project
build: ## Build the Go project
	go build -o bin/random_data_loader ./cmd

## Run tests
 test: ## Run all tests
	go test ./...

## Run golangci-lint
lint: ## Run golangci-lint
	golangci-lint run ./...

## Run golangci-lint with --fix
lint-fix: ## Run golangci-lint with --fix
	golangci-lint run --fix ./...

## Run go mod tidy
 tidy: ## Clean up go.mod and go.sum
	go mod tidy

## Clean build artifacts
clean: ## Remove build output
	rm -rf bin

## Run the main application
run: build ## Build and run the application
	./bin/random_data_loader

## Start Docker containers
env-up: ## Start all services using docker-compose
	docker compose up -d

## Stop Docker containers
env-down: ## Stop all services using docker-compose
	docker compose down
