build:
	go build -o gotgz ./cmd/gotgz

install:
	go install ./cmd/gotgz

lint:
	golangci-lint run --timeout 10m

format:
	gofmt -w -s .
	go fix ./...

test:
	docker compose down
	docker compose up -d --wait
	GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 go test -v ./...
	docker compose down
