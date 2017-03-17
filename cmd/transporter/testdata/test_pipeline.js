m = mongodb({"uri": "mongo://localhost:27017"})
t.Source("source", m)
  .Transform("trans", transformer({filename: "pipeline.js"}))
  .Save("sink", elasticsearch({uri:"http://localhost:9200"}))