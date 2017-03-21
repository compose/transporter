enron_source_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SOURCE_USER}:${MONGODB_ENRON_SOURCE_PASSWORD}@${MONGODB_ENRON_SOURCE_URI}/enron",
  "tail": false
})

enron_sink_mongo = mongodb({
  "uri": "mongodb://${MONGODB_ENRON_SINK_USER}:${MONGODB_ENRON_SINK_PASSWORD}@${MONGODB_ENRON_SINK_URI}/enron",
  "ssl": true,
  "bulk": true,
  "wc": 2,
  "fsync": true
})

t.Source("enron_source_mongo", enron_source_mongo, "emails")
  .Save("enron_sink_mongo", enron_sink_mongo, "emails");
