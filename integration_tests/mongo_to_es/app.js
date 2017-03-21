enron_source_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SOURCE_USER}:${MONGODB_ENRON_SOURCE_PASSWORD}@${MONGODB_ENRON_SOURCE_URI}/enron",
  "tail": false
})

enron_sink_es = elasticsearch({
  "uri": "https://${ES_ENRON_SINK_USER}:${ES_ENRON_SINK_PASSWORD}@${ES_ENRON_SINK_URI}/enron"
})

t.Source("enron_source_mongo", enron_source_mongo, "emails")
  .Save("enron_sink_es", enron_sink_es, "emails");
