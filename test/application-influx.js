
pipeline = Source({name:"localmongo", namespace: "gru-development.backups"})
  .transform({filename: "transformers/influx_test.js"})
  .save({name:"timeseries", namespace: "compose.backups"})
