
// var transporter = Transport({name:"mongodb-production", namespace: "compose.milestones2"})
// transporter = transporter.transform("transformers/transform1.js")
// transporter = transporter.transform("transformers/transform2.js")
// x = transporter.save({name:"supernick", namespace: "something/posts2"});

// console.log(JSON.stringify(x));

// Transport({name:"mongodb-production", namespace: "metrics.hits"}).save({name:"supernick", namespace: "somethingelse/posts4"});
// Transport({name:"localmongo", namespace: "boom.foo"}).save({name:"tofile", namespace: ""})

Transport({name:"crapfile", namespace: ""}).save({name:"stdout", namespace: ""})