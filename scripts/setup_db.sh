#!/usr/bin/env bash

set -e

case "$TESTDIR" in
  adaptor/*)
    # Communication to the docker-hosted DB will happen on that hostname
    echo "127.0.0.1 transporter-db" | sudo tee -a /etc/hosts
    ;;
esac


case "$TESTDIR" in
'adaptor/mongodb/...')
  # TODO: migrate all adaptors to that format
  scripts/run_db_in_docker.sh mongodb $MONGODB_VERSION
;;
'adaptor/postgres/...')
  scripts/run_db_in_docker.sh postgres $POSTGRESQL_VERSION
;;
'adaptor/elasticsearch/...')
  echo "Configuring elasticsearch"
  mkdir -p /tmp/elasticsearch/config
  cp -r config/elasticsearch/* /tmp/elasticsearch/config/
  sudo sysctl -w vm.max_map_count=262144
  docker run --rm --privileged=true -p 127.0.0.1:9205:9205 -v "/tmp/elasticsearch/config/v5:/usr/share/elasticsearch/config" -e ES_JAVA_OPTS='-Xms1g -Xmx1g' elasticsearch:5.1.2 elasticsearch >& /dev/null &
  docker run --rm --privileged=true -p 127.0.0.1:9202:9202 -v "/tmp/elasticsearch/config/v2:/usr/share/elasticsearch/config" -e ES_JAVA_OPTS='-Xms1g -Xmx1g' elasticsearch:2.4.4 elasticsearch >& /dev/null &
  docker run --rm --privileged=true -p 127.0.0.1:9201:9201 -v "/tmp/elasticsearch/config/v1:/usr/share/elasticsearch/config" -e ES_JAVA_OPTS='-Xms1g -Xmx1g' elasticsearch:1.7.6 elasticsearch >& /dev/null &

  wait_on_port 9205
  wait_on_port 9202
  wait_on_port 9201
;;
'adaptor/rabbitmq/...')
  echo "Configuring rabbitmq"
  mkdir -p /tmp/rabbitmq
  cp -r config/rabbitmq/certs/* /tmp/rabbitmq/

  mkdir -p /tmp/rabbitmq_bad_cert
  cp -r config/rethinkdb/certs/* /tmp/rabbitmq_bad_cert/

  sudo apt-get update -qq
  sudo apt-get install -y software-properties-common ssh
  # Install haproxy-2.0
  sudo add-apt-repository ppa:vbernat/haproxy-2.0 -y
  sudo apt-get update -qq
  sudo apt-get install -y haproxy rabbitmq-server
  sudo service rabbitmq-server start
  wait_on_port 5672
  sudo haproxy -f config/rabbitmq/haproxy.cfg -db &
;;
'adaptor/rethinkdb/...')
  source /etc/lsb-release && echo "deb https://download.rethinkdb.com/repository/ubuntu-$DISTRIB_CODENAME $DISTRIB_CODENAME main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list
  wget -qO- https://download.rethinkdb.com/repository/raw/pubkey.gpg | sudo apt-key add -
  sudo apt-get update
  sudo apt-get install -y rethinkdb
  sleep 10

  echo "Configuring rethinkdb"
  mkdir -p /tmp/rethinkdb
  cp -r config/rethinkdb/certs/* /tmp/rethinkdb/

  mkdir -p /tmp/rethinkdb_ssl
  rethinkdb --config-file config/rethinkdb/configurations/ssl.conf >& /dev/null &

  mkdir -p /tmp/rethinkdb_auth
  rethinkdb --initial-password admin123 --config-file config/rethinkdb/configurations/auth.conf >& /dev/null &
;;
*)
  echo "no setup required for $TESTDIR"
;;
esac
