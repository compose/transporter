pipeline = Source({name:"foofile"})

pipeline.transform({filename: "transformers/passthrough_and_log.js"}).save({name:"foofile2"})
pipeline.save({name:"localmongo", namespace: "boom.bas"})
pipeline.save({name:"localmongo", namespace: "boom.baz"})
