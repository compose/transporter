#!/usr/bin/env bash

set -ev

curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.33.0

if [[ $TESTDIR == adaptor/mongodb* ]]; then
  sudo pip install "mongo-orchestration>=0.6.7,<1.0"

  wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz

  mkdir -p /tmp/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION

  tar xfz mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz -C /tmp

  rm mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz

  export PATH=/tmp/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION/bin:$PATH

  mongod --version
fi

go mod download
