# RethinkDB adaptor

The [RethinkDB](https://github.com/rethinkdb/rethinkdb) adaptor is capable of reading/tailing tables and
receiving data for inserts.

### Configuration:
```yaml
- rethink:
    type: rethinkdb
    uri: rethink://127.0.0.1:28015/
    # timeout: 30s
    # tail: false
    # ssl: false
    # cacerts: ["/path/to/cert.pem"]
```
