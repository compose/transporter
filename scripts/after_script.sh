#!/usr/bin/env bash

set -ev

if [[ $TRAVIS_EVENT_TYPE != 'cron' ]]; then
  if [[ $TESTDIR == pkg/adaptor/mongodb* ]]; then
    mongo-orchestration stop
  elif [[ $TESTDIR == pkg/adaptor/rabbitmq* ]]; then
    haproxy_pid=$(ps -ef | grep "[h]aproxy" | awk '{print $2}')
    kill $haproxy_pid
  fi
fi
