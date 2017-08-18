#!/usr/bin/env bash

set -e

echo "running tests in $TESTDIR"

echo "" > coverage.txt

IFS=', ' read -r -a test_dir <<< "$TESTDIR"
for t in "${test_dir[@]}"; do
  echo "testing $t"
  for d in $(go list ./$t); do
      go test -v -coverprofile=profile.out -covermode=atomic $d -log.level=error
      if [ -f profile.out ]; then
          cat profile.out >> coverage.txt
          rm profile.out
      fi
  done
done
