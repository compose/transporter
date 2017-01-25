#!/usr/bin/env bash

set -e

# setup configurations

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
