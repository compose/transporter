
pipeline = Source({name:"crapfile"})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"es", namespace: "test.crap"})
