# Research AI Agent - Development Commands

# Build and run all services
build:
    docker compose up --build

# Run in detached mode
up:
    docker compose up -d

# Stop all services
down:
    docker compose down

# Stop and remove volumes
clean:
    docker compose down -v

# View logs
logs:
    docker compose logs -f app

# Run migrations inside container
migrate:
    docker compose exec app /app/migrate

# Generate SQLC code
sqlc:
    sqlc generate

# Run tests
test:
    go test -v ./...

# Run tests with coverage
test-coverage:
    go test -v -coverprofile=coverage.txt ./... && go tool cover -html=coverage.txt -o coverage.html

# Build binary locally
build-local:
    go build -o bin/server ./cmd/server

# Run locally (requires services running)
run:
    go run ./cmd/server

# Format code
fmt:
    go fmt ./...

# Lint (requires golangci-lint)
lint:
    golangci-lint run ./...

# Check for issues
check:
    go vet ./...
    go build ./...

# Install sqlc (if not installed)
install-sqlc:
    go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

# Install golangci-lint (if not installed)
install-lint:
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Development setup
setup: install-sqlc
    cp .env.example .env
    @echo "Edit .env with your API keys, then run: just build"
