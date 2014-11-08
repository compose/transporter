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
  crapfile:
    type: file
    uri: file:///tmp/crap
  stdout:
    type: file
    uri: stdout://
```

There is also a sample 'application.js' in test/application.js.  The application is responsible for building transporter pipelines.
Given the above config, this Transporter will copy from a file (in /tmp/crap) to stdout.
```js
Transport({name:"crapfile", namespace: ""}).save({name:"stdout", namespace: ""})
```

This will copy from the local mongo to a file on the local disk
```js
Transport({name:"localmongo", namespace: "boom.foo"}).save({name:"tofile", namespace: ""})
```

Transformers are also configured in the application.js as follows, however transformers have not yet been implemented
```js
var transporter = Transport({name:"mongodb-production", namespace: "compose.milestones2"})
transporter = transporter.transform("transformers/transform1.js")
transporter = transporter.transform("transformers/transform2.js")
transporter.save({name:"supernick", namespace: "something/posts2"});

```
Run
---

- list `./transporter --config ./test/config.yaml list`
- run `./transporter --config ./test/config.yaml run ./test/application.js`


