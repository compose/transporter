
// create a pipeline that reads documents from a file, transforms them, and writes them
pipeline = Source({name:"crapfile"}).transform("transformers/passthrough_and_log.js").save({name:"errorfile"})

