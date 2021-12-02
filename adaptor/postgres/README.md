# PostgreSQL adaptor

The [PostgreSQL](https://www.postgresql.org/) adaptor is capable of reading/tailing tables
using logical decoding and receiving data for inserts.

### Configuration:
```javascript
pg = postgres({
  "uri": "postgres://127.0.0.1:5432/test"
})
```

### Permissions

Postgres as a transporter source uses [Logical Decoding](https://www.postgresql.org/docs/current/static/logicaldecoding-explanation.html) which requires the user account to have `superuser` or `replication` permissions.

## Run adaptor test

### Spin up required containers

You'll need those ports on your local machine: `5432`

So make sure to kill anything that might use them (like a local postgres instance)

```sh
# From transporter's root folder
version=12
# Pay attention to a WARNING telling you to add a line to /etc/hosts in the following command
scripts/run_db_in_docker.sh postgres $version
```

### Run the tests

```sh
# From transporter's root folder
go test -v ./adaptor/postgres/
```

### Tear down containers

Once you're done

```sh
TESTDIR=adaptor/postgres scripts/teardown_db_in_docker.sh
```
