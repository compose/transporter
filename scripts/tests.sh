#!/usr/bin/env bash

set -e

if [[ "$TRAVIS_EVENT_TYPE" == "cron" && "$TESTDIR" == integration_tests* ]]; then
  echo "running integration tests in $TESTDIR"

  go install ./cmd/transporter/...

  go test -v ./$TESTDIR/... -cleanup=true -tags=integration

  transporter run -config integration_tests/config.yml $TESTDIR/app.js

  go test -v ./$TESTDIR/... -tags=integration -log.level=error
elif [[ "$TRAVIS_EVENT_TYPE" != "cron" && "$TESTDIR" == pkg* ]]; then
  echo "running tests in $TESTDIR"

  echo "" > coverage.txt

  IFS=', ' read -r -a test_dir <<< "$TESTDIR"
  for t in "${test_dir[@]}"; do
    echo "testing $t"
    for d in $(go list ./$t); do
        go test -v -coverprofile=profile.out -covermode=atomic $d
        if [ -f profile.out ]; then
            cat profile.out >> coverage.txt
            rm profile.out
        fi
    done
  done
else
  echo "skipping $TESTDIR"
fi
