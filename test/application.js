
// create a pipeline that reads documents from a file, transforms them, and writes them
Source({name:"localmongo", namespace:"boom.foo"}).transform({name: "simpletrans", filename: "transformers/passthrough_and_log.js", debug: false}).save({name:"loosefile", uri:"file:///tmp/foo"})
