#!/usr/bin/env bash

set -ev

curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.33.0
go mod download

./scripts/setup_db.sh
