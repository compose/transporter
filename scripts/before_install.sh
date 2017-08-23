#!/usr/bin/env bash

set -ev

if [[ $TESTDIR == adaptor/mongodb* ]]; then
  pip install "mongo-orchestration>=0.6.7,<1.0"

  wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz

  mkdir -p /tmp/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION

  tar xfz mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz -C /tmp

  rm mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz

  export PATH=/tmp/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION/bin:$PATH

  mongod --version
fi
