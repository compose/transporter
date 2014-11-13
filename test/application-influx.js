transporter = Transporter()

pipeline = Source({name:"localmongo", namespace: "gru-development.backups"})
  .transform("transformers/influx_test.js")
  .save({name:"timeseries", namespace: "compose.backups"})

transporter.add(pipeline)
