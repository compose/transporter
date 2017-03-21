m = mongodb({uri: "${TEST_MONGO_URI}"})
t.Source("source", m)
  .Transform("trans", transformer({filename: "pipeline.js"}))
  .Save("sink", elasticsearch({uri:"http://localhost:9200"}))