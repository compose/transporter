
// create a transporter
t = Transporter()

// create a pipeline
pipeline = Source({name:"crapfile", namespace: ""})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"crapfile2", namespace: "c"})
  .transform("transformers/passthrough_and_log2.js")
  .save({name:"localmongo", namespace: "boom.crap"})

// add the pipeline to the transporter
t.add(pipeline)


