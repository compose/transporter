pipeline = Source({name:"foofile"})

pipeline.transform({filename: "transformers/passthrough_and_log.js"}).sink({name:"foofile2"})
pipeline.sink({name:"localmongo", namespace: "boom.bas"})
pipeline.sink({name:"localmongo", namespace: "boom.baz"})
