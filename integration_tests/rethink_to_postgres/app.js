enron_source_rethink = rethinkdb({
  "uri": "rethink://admin:${RETHINKDB_ENRON_SOURCE_PASSWORD}@${RETHINKDB_ENRON_SOURCE_URI}/enron",
  "ssl": true,
  namespace: "enron.emails"
})

enron_sink_postgres = postgres({
  "uri": "postgres://${POSTGRES_ENRON_SINK_USER}:${POSTGRES_ENRON_SINK_PASSWORD}@${POSTGRES_ENRON_SINK_URI}",
  "namespace": "enron.emails"
})

t.Source(enron_source_rethink).Save(enron_sink_postgres);
