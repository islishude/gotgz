all: build lint fmt test

build:
	go build -o gotgz ./cmd/gotgz

install:
	go install -trimpath -ldflags="-s -w" ./cmd/gotgz

lint:
	go vet ./...
	golangci-lint run --timeout 10m

fmt:
	gofmt -w -s .
	go fix ./...

test: unit-test integration-test e2e-test

unit-test:
	go test -v -race -count=1 -coverprofile=coverage.txt ./...

integration-test:
	@set -e; \
		docker compose down; \
		docker compose up -d --wait; \
		trap 'docker compose down' EXIT; \
		GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 go test -v -race -count=1 -tags=integration ./...; \
		docker compose down

e2e-test:
	go test -v -race -count=1 -tags=e2e ./cmd/gotgz

s3mock:
	docker compose down
	docker compose up -d --wait
