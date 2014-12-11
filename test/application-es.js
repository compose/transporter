
pipeline = Source({name:"testfile"})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"es", namespace: "test.test"})
