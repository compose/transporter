// create a pipeline
pipeline = Source({name:"localmongo", namespace: "boom.foo", tail: true})

// transform a few things:
// * transform _id as an ObjectId to id as a string
// * set namespace to a value suitable for Postgres
pipeline.transform({
  filename: "transformers/id_to_id.js",
  namespace: "boom..*"
}).save({
  name: "postgres-dest",
  namespace: "boomdest..*"
})
