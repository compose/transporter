Transporter

Build
-----
`go build -a ./cmd/...`


Configure
---------
there is a sample config in test/config.yaml.  The config defines the endpoints, (either sources or sinks) that are available to the application.
```yaml
api:
  interval: 1 # number of milliseconds between metrics pings
  uri: "http://requestb.in/1a0zlf11"
nodes:
  localmongo:
    type: mongo
    uri: mongodb://localhost/boom
  supernick:
    type: elasticsearch
    uri: http://10.0.0.1,10.0.0.2:9200/indexname
  debug:
    type: file
    uri: stdout://
  foofile:
    type: file
    uri: file:///tmp/foo
  stdout:
    type: file
    uri: stdout://
```

There is also a sample 'application.js' in test/application.js.  The application is responsible for building transporter pipelines.
Given the above config, this Transporter application.js will copy from a file (in /tmp/foo) to stdout.
```js
Source({name:"foofile"}).save({name:"stdout"})

```

This application.js will copy from the local mongo to a file on the local disk
```js
Source({name:"localmongo", namespace: "boom.foo"}).save({name:"tofile"})
```

Transformers can also configured in the application.js as follows
```js
var pipeline = Source({name:"mongodb-production", namespace: "compose.milestones2"})
pipeline = pipeline.transform("transformers/transform1.js").transform("transformers/transform2.js")
pipeline.save({name:"supernick", namespace: "something/posts2"});

```
Run
---

- list `transporter list --config ./test/config.yaml`
- run `transporter run --config ./test/config.yaml ./test/application.js`
- eval `transporter eval --config ./test/config.yaml 'Source({name:"localmongo", namespace: "boom.foo"}).save({name:"tofile"})' `
- test `transporter test --config ./test/config.yaml test/application.js `

Contributing to Transporter
======================

[![Circle CI](https://circleci.com/gh/compose/transporter/tree/master.png?style=badge)](https://circleci.com/gh/compose/transporter/tree/master)

Want to help out with Transporter? Great! There are instructions to get you
started [here](CONTRIBUTING.md).

Licensing
=========
Transporter is licensed under the New BSD. See LICENSE for full license text.
