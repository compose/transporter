# PostgreSQL adaptor

The [PostgreSQL](https://www.postgresql.org/) adaptor is capable of reading/tailing tables 
using logical decoding and receiving data for inserts.

### Configuration:
```javascript
pg = postgres({
  "uri": "postgres://127.0.0.1:5432/test"
})
```

#### Case Sensitive Table Identifiers:
If your database contains case sensitive table identifiers, you can tell transporter to add quotes to every table name when sending queries.

```javascript
pg = postgres({
  "uri": "postgres://127.0.0.1:5432/test",
  "case_sensitive_identifiers": true
})
```

If you enable this option, just keep in mind that now every namespace in every message will now also contain quotes. For example, if you have a namespace filter in the pipeline like `'/^myschema.mytable$/'`, you will now need to change it to `'/^myschema."mytable"$/'`.

### Permissions

Postgres as a transporter source uses [Logical Decoding](https://www.postgresql.org/docs/current/static/logicaldecoding-explanation.html) which requires the user account to have `superuser` or `replication` permissions. 

**Warning!** PostgreSQL on Compose platform does not support superuser permissions so it is not possible to use a Compose PostgreSQL database as a transporter source.
