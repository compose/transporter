
// create a pipeline that reads documents from a file, transforms them, and writes them
pipeline = Source({name:"foofile"}).transform({name: "simpletrans", filename: "transformers/passthrough_and_log.js", debug: false}).save({name:"errorfile"})
