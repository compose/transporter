enron_source_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SOURCE_USER}:${MONGODB_ENRON_SOURCE_PASSWORD}@${MONGODB_ENRON_SOURCE_URI}/enron",
  "tail": false,
  "namespace": "enron.emails"
})

enron_sink_es = elasticsearch({
  "uri": "https://${ES_ENRON_SINK_USER}:${ES_ENRON_SINK_PASSWORD}@${ES_ENRON_SINK_URI}",
  "namespace": "enron.emails"
})

t.Source(enron_source_mongo).Save(enron_sink_es);
