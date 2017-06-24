enron_source_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SOURCE_USER}:${MONGODB_ENRON_SOURCE_PASSWORD}@${MONGODB_ENRON_SOURCE_URI}/enron",
  "tail": false
})

enron_sink_rethink = rethinkdb({
  "uri": "rethink://admin:${RETHINKDB_ENRON_SINK_PASSWORD}@${RETHINKDB_ENRON_SINK_URI}/enron",
  "ssl": true
})

var config = {
  "write_timeout": "30s",
}

t.Config(config)
  .Source("enron_source_mongo", enron_source_mongo, "emails")
  .Save("enron_sink_rethink", enron_sink_rethink, "emails");
