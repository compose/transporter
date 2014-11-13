
// create a transporter
t = Transporter()

// create a pipeline
pipeline = Source({name:"localmongo", namespace: "boom.foo"})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"stdout", namespace: ""})

t.add(pipeline)

