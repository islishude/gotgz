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

test: s3mock
	GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 go test -v -race -count=1 -coverprofile=coverage.txt ./...
	docker compose down

unit-test:
	go test -v -race ./...

s3mock:
	docker compose down
	docker compose up -d --wait
