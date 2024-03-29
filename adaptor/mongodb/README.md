# MongoDB adaptor

The [MongoDB](https://www.mongodb.com/) adaptor is capable of reading/tailing collections and receiving data for inserts.

***NOTE*** You may want to check your collections to ensure the proper index(es) are in place or performance may suffer.

## Example

```javascript
m = mongodb({
  "uri": "mongodb://127.0.0.1:27017/test"
  // "timeout": "30s",
  // "tail": false,
  // "ssl": false,
  // "cacerts": ["/path/to/cert.pem"],
  // "wc": 1,
  // "fsync": false,
  // "bulk": false,
  // "collection_filters": "{\"foo\": {\"i\": {\"$gt\": 10}}}"
})
```

## Options

| Parameter          | Description                                                  | Default                        |
| ------------------ | ------------------------------------------------------------ | ------------------------------ |
| uri                | Defines the full connection string of the MongoDB database.  | mongodb://127.0.0.1:27017/test |
| timeout            | Overrides the default session timeout and should be parseable by time.ParseDuration | 10s                            |
| tail               | Whether the source connection will listen for updates after the initial sync (requires oplog access) | false                          |
| ssl                | Configures the database connection to connect via TLS        | false                          |
| cacerts            | Configures the RootCAs for the underlying TLS connection     | []                             |
| wc                 | Configures the write concern option for the session          | 0                              |
| fsync              | Whether the server will wait for Fsync to complete before returning a response | false                          |
| bulk               | Whether the sink connection will use bulk inserts rather than writing one record at a time. | false                          |
| collection_filters | A JSON string where the top level key is the collection name and its value  is a query that will be used when iterating the collection. The commented out example above  would only  include documents where the `i` field had a value greater than `10` | {}                             |

## Run adaptor test

### Spin up required containers

You'll need those ports on your local machine: `10000, 10001, 27017, 27018, 11112, 29017, 15000, 20000`

So make sure to kill anything that might use them (like a local mongo instance)

```sh
# From transporter's root folder
version=3.2.11
# Pay attention to a WARNING telling you to add a line to /etc/hosts in the following command
scripts/run_db_in_docker.sh mongodb $version
```

### Run the tests

```sh
# From transporter's root folder
go test -v ./adaptor/mongodb/
```

### Tear down containers

Once you're done

```sh
TESTDIR=adaptor/mongodb scripts/teardown_db_in_docker.sh
```
