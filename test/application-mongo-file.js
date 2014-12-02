
// create a pipeline
pipeline = Source({name:"localmongo", namespace: "boom.foo"})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"stdout"})

