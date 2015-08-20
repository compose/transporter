# Move data from Postgres to RethinkDB

This tutorial will transfer data from a Postgres database to a RethinkDB.

1. Setup a working directory for this transporter example

2. Download the `transporter` binary into the directory from https://github.com/Winslett/transporter/releases/tag/beta-postgres

3. Configure local Postgres

  * Install postgresql (e.g. brew install postgresql)
  * Make a data directory - `mkdir data`
  * Initialise for postgresql - `initdb -D data -U postgres`
  * in the `data/postgresql.conf` file, uncomment and change the following:
  ```
  wal_level=logical
  max_replication_slots=1
  ```
  * Run your PostgreSQL database - `postgresql -D data &`
  * Run the following to initialize your source db (connect with `psql -U postgres postgres`):
  ```sql
  CREATE DATABASE my_source_db;
  \connect my_source_db

  CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255), created_at TIMESTAMP);
  INSERT INTO users (name, created_at) VALUES ('Chris', now()), ('Kyle', now()), ('Michele', now());
  
  SELECT * FROM pg_create_logical_replication_slot('rethink_transporter', 'test_decoding');
  ```

4. Start local Rethink

  * Install with `brew install rethinkdb`
  * Run with `/usr/local/bin/rethinkdb`
  * Browse to http://localhost:8080
  * create a new database called 'myDestDB'
  * create a table for `users` with Primary Index for `_id`

5. Create a file called `config.yaml`, which will hold the data adapter
   configurations

  ```yaml
  nodes:
    postgres:
      type: postgres
      uri: "host=localhost sslmode=disable dbname=my_source_db"
    rethink:
      type: rethinkdb
      uri: rethink://localhost:28015/
  ```

6. Create a file called `application.js` which will define the data movement:

  ```js
  pipeline = Source({
    name: "postgres",
    namespace: "my_source_db.public..*",
    tail: true,
    replication_slot: "rethink_transporter"
  })

  pipeline.save({
    name: "rethink",
    namespace: "myDestDB..*"
  })
  ```

7. Run transporter:

  ```
  ./transporter run --config config.yaml application.js
  ```

8. Go to Rethink and run query for:

  ```
  r.db("myDestDB").table("users")
  ```

9. Go to Postgres (`psql -U postgres postgres` then `\connect my_source_db`)
   and update users:

  ```sql
  UPDATE users SET name = 'Jason' WHERE id = 1;
  ```

10. Go back to Rethink, and rerun your query.  The name should have
   updated.  You'll see duplicates.  The problem is mis-matching primary
   keys.  This is where transformations can help.

11.  Create a `transform.js` file like this:

  ```js
  module.exports = function(doc) {
    doc.data._id = doc.data["id"]
    delete doc.data["id"]
    return doc
  }
  ```

12. Replace the old save with the following lines in the `application.js`:

  ```js
  pipeline.transform({
    namespace: "public..*",
    filename: "transform.js"
  }).save({
    name: "rethink",
    namespace: "myDestDB..*"
  })
  ```

13. Drop the RethinkDB table and re-create. 
    Use the UI or run
 ```js
 r.db("myDestDB").tableDrop("users")
 r.db("myDestDB").tableCreate("users",{ primaryKey:"_id" })
 ```
   Then, rerun transporter.

14. Go to Postgres, and re-update user:

  ```sql
  UPDATE users SET name = 'Chris' WHERE id = 1;
  ```
