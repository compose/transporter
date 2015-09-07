
// create a pipeline
pipeline = Source({name:"localmongo", namespace: "boom.foo", tail: true})
    .transform({filename: "transformers/passthrough_and_log.js"})
    .save({name:"localmongo2",namespace: "boom.foo"})

