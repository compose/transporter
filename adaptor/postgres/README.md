# PostgreSQL adaptor

The [PostgreSQL](https://www.postgresql.org/) adaptor is capable of reading/tailing tables 
using logical decoding and receiving data for inserts.

### Configuration:
```javascript
pg = postgres({
  "uri": "postgres://127.0.0.1:5432/test"
})
```
