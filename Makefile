build:
	go build -o gotgz ./cmd/gotgz

install:
	go install ./cmd/gotgz

lint:
	go vet ./...
	golangci-lint run --timeout 10m

fmt:
	gofmt -w -s .
	go fix ./...

test: build lint fmt localstack
	GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 go test -v -race ./...
	docker compose down

localstack:
	docker compose down
	docker compose up -d --wait
