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

**Warning!** PostgreSQL on Compose platform does not support superuser permissions so it is not possible to use a Compose PostgreSQL database as a transporter source.
