# Paper Scout - Development Commands

# Build/start dependency services
build:
    docker compose up --build -d

# Run dependencies in detached mode
up:
    docker compose up -d

# Stop all services
down:
    docker compose down

# Stop and remove volumes
clean:
    docker compose down -v

# View dependency logs
logs:
    docker compose logs -f postgres redis qdrant docling

# Run Goose migrations (requires goose)
migrate:
    goose -dir migrations postgres "host=localhost port=5432 user=research password=research123 dbname=research_agent sslmode=disable" up

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

# Run API locally (requires services running)
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
    @test -z "$(gofmt -l .)"
    go vet ./...
    go test ./...
    go build ./...

# Rebuild the configured embedding generation with durable, recoverable activation.
reindex:
    go run ./cmd/reindex

# Install sqlc (if not installed)
install-sqlc:
    go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

# Install golangci-lint (if not installed)
install-lint:
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Install air for hot reload (if not installed)
install-air:
    go install github.com/air-verse/air@latest

# Run with hot reload (requires air)
dev:
    air

# Development setup
setup: install-sqlc install-air
    cp .env.example .env
    @echo "Edit .env with your API keys, then run: just dev"
