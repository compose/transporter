# MongoDB adaptor

The [MongoDB](https://www.mongodb.com/) adaptor is capable of reading/tailing collections and
receiving data for inserts.

### Configuration:
```yaml
- mongodb:
    type: mongo
    uri: mongodb://127.0.0.1:27017/test
    namespace: test.data
    tail: false
```
