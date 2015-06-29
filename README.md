Transporter
===========

Build
-----
`go build -a ./cmd/...`


Configure
---------
There is a sample config in test/config.yaml.  The config defines the endpoints, (either sources or sinks) that are available to the application.
```yaml
api:
  interval: 60s # time interval between metrics posts to the api endpoint
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

Transformers can also be configured in the application.js as follows
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

Complete beginners guide (OS X)
---
- ensure you have mercurial installed as it is required for a dependency
    - using the homebrew package manager `brew install hg` [Homebrew Guide/Install](http://brew.sh/)
- install the Mac OS X binary build from https://golang.org/dl/
- follow instructions on http://golang.org/doc/install
- VERY IMPORANT: Go has a required directory structure which the GOPATH needs to point to. Instructions can be found on http://golang.org/doc/code.html or by typing `go help gopath` in terminal.
- setup the directory structure in $GOPATH
    - `cd $GOPATH; mkdir src pkg bin`
    - create the github.com path and compose `mkdir -p src/github.com/compose; cd src/github.com/compose`
    - clone transporter `git clone https://github.com/compose/transporter.git; cd transporter`
    - run go get to get all the dependencies `go get -a ./cmd/...`
    - now you can build `go build -a ./cmd/...`

At this point you should be able to run transporter via `$GOPATH/bin/transporter`,  you may need to add $GOPATH to your PATH environment variable. Something along the lines of `export PATH="$GOPATH/bin:$PATH"` should work.

Contributing to Transporter
======================

Want to help out with Transporter? Great! There are instructions to get you
started [here](CONTRIBUTING.md).

Licensing
=========
Transporter is licensed under the New BSD License. See LICENSE for full license text.
