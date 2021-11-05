#!/usr/bin/env bash

set -e

adaptor=$1
version=$2

wait_on_logs () {
  pattern=${1}
  i=0

  docker ps --filter "ancestor=transporter_mongodb" -q
  container_id=`docker ps --filter "ancestor=transporter_mongodb" -q`

  # docker logs exits 1 on Github Actions for some reason
  if [[ -n $GITHUB_WORKFLOW ]]; then
    sleep 60
    return
  fi

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
  echo "WARNING: your /etc/hosts doesn't include a way to connect to the dockerized db. Please add the following line by running this command:"
  echo "echo 127.0.0.1 transporter-db >> /etc/hosts"
fi

echo "Waiting on container to be ready"

case "$adaptor" in
  'mongodb')
    wait_on_logs "MongoDB: setup complete"
;;
esac
