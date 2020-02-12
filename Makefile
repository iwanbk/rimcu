check: build test lint

build:
	go build -v ./...

test:
	gotest -race -cover ./...

lint:
	golint  -set_exit_status *.go
	golint  -set_exit_status resp2/*.go
	golangci-lint run ./... 