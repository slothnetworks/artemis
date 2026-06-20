# Format all files
fmt:
	@echo "==> Formatting source"
	@gofmt -s -w $(shell find . -type f -name '*.go' -not -path "./vendor/*")
	@echo "==> Done"
.PHONY: fmt

# Tidy the go.mod file
tidy:
	@echo "==> Cleaning go.mod"
	@go mod tidy
	@echo "==> Done"
.PHONY: tidy

# Generate protobuf / gRPC code (requires buf, protoc-gen-go, protoc-gen-go-grpc)
proto:
	@echo "==> Generating protobuf"
	@buf lint
	@buf generate
	@echo "==> Done"
.PHONY: proto

# Generate Swagger / OpenAPI docs (requires swag)
swagger:
	@echo "==> Generating swagger"
	@swag init -g internal/api/api.go --parseInternal --parseDependency --exclude sample-work,web-sample -o docs/swagger
	@echo "==> Done"
.PHONY: swagger

# Run all tests
test:
	@go test -cover -race ./...
.PHONY: test

# Lint the project
lint:
	@golangci-lint run ./...
.PHONY: lint

# Build the commands
build:
	@goreleaser release --clean --snapshot
.PHONY: build
