
// create a pipeline that reads documents from a file, transforms them, and writes them
pipeline = Source({
  name:"postgres",
  namespace:"fizz.public..*",
  tail: true,
  replication_slot: "fizz_transporter"
})

pipeline.transform({
  namespace: "boom..*",
  filename: "transformers/to_id.js"
}).save({
  name:"localmongo",
  namespace: "boom..*"
})

pipeline.save({
  name: "rethink1",
  namespace: "pgToRethink..*"
})

pipeline.save({
  name: "postgres-dest",
  namespace: "destfizz..*"
})

pipeline.save({
  name: "foofile",
  namespace: "public..*"
})
