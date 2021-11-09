#!/usr/bin/env bash

set -e

case "$TESTDIR" in
  adaptor/*)
    adaptor=`cut -d "/" -f2 <<< $TESTDIR`
    cd config/$adaptor/test_setup
    docker-compose down
    ;;
esac
