install:
	go install ./gotgz

test:
	docker compose down
	docker compose up -d --wait
	S_CI=true go test -v ./...
	docker compose down
