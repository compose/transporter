[![Build Status](https://travis-ci.org/compose/transporter.svg?branch=master)](https://travis-ci.org/compose/transporter) [![Go Report Card](https://goreportcard.com/badge/github.com/compose/transporter)](https://goreportcard.com/report/github.com/compose/transporter) [![Coverage Status](https://coveralls.io/repos/github/compose/transporter/badge.svg?branch=master)](https://coveralls.io/github/compose/transporter?master) [![Docker Repository on Quay](https://quay.io/repository/compose/transporter/status "Docker Repository on Quay")](https://quay.io/repository/compose/transporter)

Compose helps with database transformations from one store to another.  It can also sync from one to another or several stores.

Transporter
===========

Build
-----
```
go build ./cmd/transporter/...
```

Adaptors
--------
* [elasticsearch](./pkg/adaptor/elasticsearch)
* [etcd](./pkg/adaptor/etcd)
* [file](./pkg/adaptor/file)
* [mongodb](./pkg/adaptor/mongodb)
* [postgresql](./pkg/adaptor/postgres)
* [rethinkdb](./pkg/adaptor/rethinkdb)
* [transformer](./pkg/adaptor/transformer)

Configure
---------
There is a sample config in test/config.yaml.  The config defines the endpoints, (either sources or sinks) that are available to the application.
```yaml
api:
  interval: 60s # time interval between metrics posts to the api endpoint
  uri: "http://requestb.in/1a0zlf11"
nodes:
  localmongo:
    type: mongodb
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

Complete beginners guide
---

### OS X

- follow instructions on http://golang.org/doc/install
- VERY IMPORTANT: Go has a required directory structure which the GOPATH needs to point to. Instructions can be found on http://golang.org/doc/code.html or by typing `go help gopath` in terminal.
- setup the directory structure in $GOPATH
    - `cd $GOPATH; mkdir src pkg bin`
    - create the github.com path and compose `mkdir -p src/github.com/compose; cd src/github.com/compose`
    - clone transporter `git clone https://github.com/compose/transporter; cd transporter`
    - now you can build with `go build ./cmd/transporter/...`

At this point you should be able to run transporter via `$GOPATH/bin/transporter`,  you may need to add $GOPATH to your PATH environment variable. Something along the lines of `export PATH="$GOPATH/bin:$PATH"` should work.

### Vagrant

* ensure [vagrant](https://www.vagrantup.com/) is installed
* ensure [ansible](http://www.ansible.com/) is installed
* ensure either [virtual box](https://www.virtualbox.org/wiki/Downloads) or [VMWare fusion](http://www.vmware.com/products/fusion) or [VMWare Workstation](http://www.vmware.com/products/workstation) is installed

```bash
> cd transporter
> vagrant up
...
> vagrant ssh
...
vagrant> ./run-test

```

### Windows

See [READMEWINDOWS.md](https://github.com/compose/transporter/blob/master/READMEWINDOWS.md)

Transporter in the Media
===

* [Compose's articles](https://www.compose.io/articles/search/?s=transporter)

Contributing to Transporter
======================

Want to help out with Transporter? Great! There are instructions to get you
started [here](CONTRIBUTING.md).

IRC
=========
Want to talk about Transporter? Join us in #compose on irc.freenode.net!

Licensing
=========
Transporter is licensed under the New BSD License. See LICENSE for full license text.

Support and Guarantees
=========
Compose does not provide support nor guarantee stability or functionality of this tool. Please take adequate caution when using Transporter to ensure that it's the right tool for the job. Transporter may not account for failure scenarios that could lead to unexpected behavior. Always take backups, always test in dev, and always feel free to submit a PR with enhancements, features, and bug fixes.
