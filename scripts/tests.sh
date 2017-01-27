#!/usr/bin/env bash

set -e
echo "mode: count" > /tmp/coverage.out

for d in $(go list ./... | grep -v vendor); do
    go test -v -coverprofile=profile.out -covermode=count $d
    if [ -f profile.out ]; then
        cat profile.out | grep -v "mode: count" >> /tmp/coverage.out
        rm profile.out
    fi
done
