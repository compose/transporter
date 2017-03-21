enron_source_rethink = rethinkdb({
  "uri": "rethink://admin:${RETHINKDB_ENRON_SOURCE_PASSWORD}@${RETHINKDB_ENRON_SOURCE_URI}/enron",
  "ssl": true
})

enron_sink_postgres = postgres({
  "uri": "postgres://${POSTGRES_ENRON_SINK_USER}:${POSTGRES_ENRON_SINK_PASSWORD}@${POSTGRES_ENRON_SINK_URI}"
})

t.Source("enron_source_rethink", enron_source_rethink, "emails")
  .Save("enron_sink_postgres", enron_sink_postgres, "emails");
