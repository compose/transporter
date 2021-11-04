#!/usr/bin/env bash

set -e

adaptor=$1
version=$2

wait_on_logs () {
  pattern=${1}
  i=0

  container_id=`docker ps --filter "ancestor=transporter_mongodb" -q`
  until docker logs $container_id | grep "$pattern"
  do
    if [ ${i} -eq 15 ]
    then
      echo "Container not ready after 15 tries, giving up"
      exit 1
    fi


    sleep 10
    ((i++))
  done
}


SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd $SCRIPT_DIR/../config/$adaptor
docker build --build-arg VERSION=$version . --tag transporter_$adaptor
cd test_setup
docker-compose up -d

if ! grep "127.0.0.1 transporter-db" /etc/hosts > /dev/null; then
  echo "WARNING: your /etc/hosts doesn't include a way to connect to the dockerized db. Please add the following line to it:"
  echo "127.0.0.1 transporter-db"
fi

echo "Waiting on container to be ready"

case "$adaptor" in
'mongodb')
  wait_on_logs "MongoDB: setup complete"
;;
esac
