#!/usr/bin/env bash

set -e

if [[ "$TRAVIS_EVENT_TYPE" == "cron" && "$INTEGRATION_TESTS_DIR" != "default" ]]; then
  echo "running integration tests in $INTEGRATION_TESTS_DIR"

  go install ./cmd/transporter/...

  go test -v ./integration_tests/$INTEGRATION_TESTS_DIR/... -cleanup=true -tags=integration

  transporter run -config integration_tests/config.yml -log.level=error integration_tests/$INTEGRATION_TESTS_DIR/app.js 

  go test -v ./integration_tests/$INTEGRATION_TESTS_DIR/... -tags=integration -log.level=error
elif [[ "$INTEGRATION_TESTS_DIR" == "default" ]]; then
  echo "running default tests"

  echo "mode: count" > /tmp/coverage.out

  for d in $(go list ./... | grep -v vendor); do
      go test -v -coverprofile=profile.out -covermode=count $d
      if [ -f profile.out ]; then
          cat profile.out | grep -v "mode: count" >> /tmp/coverage.out
          rm profile.out
      fi
  done
else
  echo "skipping integration tests in $INTEGRATION_TESTS_DIR"
fi
