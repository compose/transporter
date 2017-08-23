#!/usr/bin/env bash

set -ev

if [[ $TESTDIR == pkg/adaptor/mongodb* ]]; then
  mongo-orchestration stop
elif [[ $TESTDIR == pkg/adaptor/rabbitmq* ]]; then
  haproxy_pid=$(ps -ef | grep "[h]aproxy" | awk '{print $2}')
  kill $haproxy_pid
fi
