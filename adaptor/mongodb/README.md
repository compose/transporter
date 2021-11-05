# MongoDB adaptor

The [MongoDB](https://www.mongodb.com/) adaptor is capable of reading/tailing collections and
receiving data for inserts.

`collection_filters` is a JSON string where the top level key is the collection name and its value
is a query that will be used when iterating the collection. The commented out example below would only
include documents where the `i` field had a value greater than `10`.

***NOTE*** You may want to check your collections to ensure the proper index(es) are in place or performance may suffer.

### Configuration:
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

## Run adaptor test

### Spin up required containers (rabbitmq, haproxy)

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
