#!/bin/sh

set -e

APP_DIR="$GOPATH/src/github.com/${GITHUB_REPOSITORY}"

mkdir -p ${APP_DIR}
cp -r ./ ${APP_DIR} && cd ${APP_DIR}


echo "build"
go build ./...

echo "make lint"
make lint

echo $TEST_REDIS_ADDRESS
echo "test"
gotest -race -coverprofile=coverage.txt -covermode=atomic ./...