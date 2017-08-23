var enron_source_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SOURCE_USER}:${MONGODB_ENRON_SOURCE_PASSWORD}@${MONGODB_ENRON_SOURCE_URI}/enron",
  "tail": false
})

var enron_sink_es = elasticsearch({
  "uri": "https://${ES_ENRON_SINK_USER}:${ES_ENRON_SINK_PASSWORD}@${ES_ENRON_SINK_URI}/enron"
})

var config = {
  "write_timeout": "30s",
}

t.Config(config)
  .Source("enron_source_mongo", enron_source_mongo, "emails")
  .Save("enron_sink_es", enron_sink_es, "emails");
