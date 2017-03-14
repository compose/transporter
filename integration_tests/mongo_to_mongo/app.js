enron_source_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SOURCE_USER}:${MONGODB_ENRON_SOURCE_PASSWORD}@${MONGODB_ENRON_SOURCE_URI}/enron",
  "tail": false,
  "namespace": "enron.emails"
})

enron_sink_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SINK_USER}:${MONGODB_ENRON_SINK_PASSWORD}@${MONGODB_ENRON_SINK_URI}/enron",
  "ssl": true,
  "bulk": true,
  "wc": 2,
  "fsync": true,
  "namespace": "enron.emails"
})

t.Source(enron_source_mongo).Save(enron_sink_mongo);
