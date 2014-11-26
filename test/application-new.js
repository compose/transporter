pipeline = Source({name:"crapfile"})

// pipeline.transform("transformers/passthrough_and_log.js").save({name:"crapfile2"})
pipeline.save({name:"localmongo", namespace: "boom.bas"})
pipeline.save({name:"localmongo", namespace: "boom.baz"})
