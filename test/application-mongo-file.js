
// create a pipeline
pipeline = Source({name:"localmongo", namespace: "boom.foo", tail: true})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"stdout"})

