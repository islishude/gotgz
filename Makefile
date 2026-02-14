build:
	go build -o gotgz ./cmd/gotgz

lint:
	golangci-lint run --timeout 10m

test:
	docker compose down
	docker compose up -d --wait
	GOTGZ_TEST_S3_ENDPOINT=http://localhost:4566 go test -v ./...
	docker compose down
