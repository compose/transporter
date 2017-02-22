#!/usr/bin/env bash

set -ev

if [[ $TRAVIS_EVENT_TYPE != 'cron' ]]; then
  if [[ $TESTDIR == pkg/adaptor/mongodb* ]]; then
    mongo-orchestration stop
  fi
fi
