enron_source_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SOURCE_USER}:${MONGODB_ENRON_SOURCE_PASSWORD}@${MONGODB_ENRON_SOURCE_URI}/enron",
  "tail": false,
  "namespace": "enron.emails"
})

enron_sink_rethink = rethinkdb({
  "uri": "rethink://admin:${RETHINKDB_ENRON_SINK_PASSWORD}@${RETHINKDB_ENRON_SINK_URI}/enron",
  "ssl": true,
  "namespace": "enron.emails"
})

t.Source(enron_source_mongo).Save(enron_sink_rethink);
