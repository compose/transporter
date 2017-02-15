#!/usr/bin/env bash

set -e

if [ "$TRAVIS_EVENT_TYPE" == "cron" ]; then
  echo "running integration tests"
  go install ./cmd/transporter/...
else
  echo "running default tests"
  
  echo "mode: count" > /tmp/coverage.out

  for d in $(go list ./... | grep -v vendor); do
      go test -v -coverprofile=profile.out -covermode=count $d
      if [ -f profile.out ]; then
          cat profile.out | grep -v "mode: count" >> /tmp/coverage.out
          rm profile.out
      fi
  done
fi
