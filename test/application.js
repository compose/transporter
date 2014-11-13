

// create a transporter
t = Transporter()

// create a pipeline
pipeline = Source({name:"crapfile", namespace: ""})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"stdout", namespace: "one"})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"stdout", namespace: "two"})

// add the pipeline to the transporter
t.add(pipeline)

// Transport({name:"crapfile", namespace: ""}).save({name:"stdout", namespace: ""})
