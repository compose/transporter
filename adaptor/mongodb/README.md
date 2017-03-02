# MongoDB adaptor

The [MongoDB](https://www.mongodb.com/) adaptor is capable of reading/tailing collections and
receiving data for inserts.

### Configuration:
```yaml
- mongodb:
    type: mongo
    uri: mongodb://127.0.0.1:27017/test
    # timeout: 30s
    # tail: false
    # ssl: false
    # cacerts: ["/path/to/cert.pem"]
    # wc: 1
    # fsync: false
    # bulk: false
```
