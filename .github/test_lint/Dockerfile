FROM golang:1.14.4

LABEL "com.github.actions.name"="build-test"
LABEL "com.github.actions.description"="run go test and build command"
LABEL "com.github.actions.icon"="terminal"
LABEL "com.github.actions.color"="blue"

LABEL "repository"="http://github.com/iwanbk/rimcu"
LABEL "homepage"="http://github.com/iwanbk/rimcu"

COPY entrypoint.sh /entrypoint.sh
RUN curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b $(go env GOPATH)/bin v1.29.0
RUN go get -v github.com/rakyll/gotest
RUN go get -v golang.org/x/lint/golint

RUN apt-get update
RUN apt-get install -y redis-server

ENTRYPOINT ["/entrypoint.sh"]
