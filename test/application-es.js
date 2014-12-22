
pipeline = Source({name:"testfile"})
  .transform({filename: "transformers/passthrough_and_log.js"})
  .save({name:"es", namespace: "test.test"})
