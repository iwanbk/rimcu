check: build test lint

build:
	go build -v ./...

test:
	gotest -race -cover ./...

lint:
	golangci-lint run ./... 