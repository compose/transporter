#!/usr/bin/env bash

set -e

case "$TESTDIR" in
'adaptor/postgres/...')
  sudo apt update
  sudo apt install -y postgresql

  until pg_isready
  do
    echo "Waiting for postgres at: $pg_uri"
    sleep 2;
  done

  echo "Configuring postgresql"
  psql -c "ALTER SYSTEM SET max_replication_slots = 4"
  psql -c "ALTER SYSTEM SET wal_level = logical"
  sudo /etc/init.d/postgresql restart; sleep 1
;;
'adaptor/elasticsearch/...')
  echo "Configuring elasticsearch"
  mkdir -p /tmp/elasticsearch/config
  cp -r config/elasticsearch/* /tmp/elasticsearch/config/
  sudo sysctl -w vm.max_map_count=262144
  docker run --rm --privileged=true -p 127.0.0.1:9205:9205 -v "/tmp/elasticsearch/config/v5:/usr/share/elasticsearch/config" -e ES_JAVA_OPTS='-Xms1g -Xmx1g' elasticsearch:5.1.2 elasticsearch >& /dev/null &
  docker run --rm --privileged=true -p 127.0.0.1:9202:9202 -v "/tmp/elasticsearch/config/v2:/usr/share/elasticsearch/config" -e ES_JAVA_OPTS='-Xms1g -Xmx1g' elasticsearch:2.4.4 elasticsearch >& /dev/null &
  docker run --rm --privileged=true -p 127.0.0.1:9201:9201 -v "/tmp/elasticsearch/config/v1:/usr/share/elasticsearch/config" -e ES_JAVA_OPTS='-Xms1g -Xmx1g' elasticsearch:1.7.6 elasticsearch >& /dev/null &
  sleep 15
;;
'adaptor/rabbitmq/...')
  echo "Configuring rabbitmq"
  mkdir -p /tmp/rabbitmq
  cp -r config/rabbitmq/certs/* /tmp/rabbitmq/

  mkdir -p /tmp/rabbitmq_bad_cert
  cp -r config/rethinkdb/certs/* /tmp/rabbitmq_bad_cert/

  sudo apt-get update -qq
  sudo apt-get install -y software-properties-common ssh
  # Install haproxy-1.5
  sudo add-apt-repository -y ppa:vbernat/haproxy-1.5
  sudo apt-get update -qq
  sudo apt-get install -y haproxy
  sudo service rabbitmq-server start
  sleep 10
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
'adaptor/mongodb/...')
  sudo pip install "mongo-orchestration>=0.6.7,<1.0"

  wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz

  mkdir -p /tmp/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION

  tar xfz mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz -C /tmp

  rm mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION.tgz

  export PATH=/tmp/mongodb-linux-x86_64-ubuntu1404-$MONGODB_VERSION/bin:$PATH

  mongod --version

  echo "Configuring mongodb"
  mkdir -p /tmp/mongodb
  cp -r config/mongodb/certs/* /tmp/mongodb/
  mongo-orchestration start -p 20000 -b 127.0.0.1

  # setup mongodb configurations

  # standard replica set w/ authentication enabled
  cat config/mongodb/configurations/rs_auth.json | curl -XPOST http://localhost:20000/v1/replica_sets -H "Content-Type: application/json" -d @-

  # basic server
  cat config/mongodb/configurations/basic.json | curl -XPOST http://localhost:20000/v1/servers -H "Content-Type: application/json" -d @-

  # basic server used for restart tests
  cat config/mongodb/configurations/reader_restart.json | curl -XPOST http://localhost:20000/v1/servers -H "Content-Type: application/json" -d @-

  # SSL server
  cat config/mongodb/configurations/ssl.json | curl -XPOST http://localhost:20000/v1/servers -H "Content-Type: application/json" -d @-

  # standard replica set
  cat config/mongodb/configurations/rs_basic.json | curl -XPOST http://localhost:20000/v1/replica_sets -H "Content-Type: application/json" -d @-

  # seed database with users and role
  mongo mongodb://transporter:transporter@127.0.0.1:10000,127.0.0.1:10001/admin?replicaSet=authRepl0 config/mongodb/scripts/setup_users_and_roles.js
;;
*)
  echo "no setup required for $TESTDIR"
;;
esac
