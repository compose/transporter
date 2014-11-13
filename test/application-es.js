transporter = Transporter()

pipeline = Source({name:"crapfile", namespace: ""})
  .transform("transformers/passthrough_and_log.js")
  .save({name:"es", namespace: "test.crap"})

transporter.add(pipeline)