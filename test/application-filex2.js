
// create a transporter
t = Transporter()

// create a pipeline
t.add(Source({name:"crapfile", namespace: ""})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"stdout", namespace: "c"}))

t.add(Source({name:"crapfile", namespace: ""})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"crapfile2", namespace: "c"}))




