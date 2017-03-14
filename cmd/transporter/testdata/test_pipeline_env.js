m = mongodb({name: "source", uri: "${TEST_MONGO_URI}"})
t.Source(m).Transform(transformer({name: "trans", filename: "pipeline.js"})).Save(elasticsearch({name: "sink", uri:"http://localhost:9200"}))