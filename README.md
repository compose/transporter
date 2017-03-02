[![Build Status](https://travis-ci.org/compose/transporter.svg?branch=master)](https://travis-ci.org/compose/transporter) [![Go Report Card](https://goreportcard.com/badge/github.com/compose/transporter)](https://goreportcard.com/report/github.com/compose/transporter) [![codecov](https://codecov.io/gh/compose/transporter/branch/master/graph/badge.svg)](https://codecov.io/gh/compose/transporter) [![Docker Repository on Quay](https://quay.io/repository/compose/transporter/status "Docker Repository on Quay")](https://quay.io/repository/compose/transporter)

Compose Transporter helps with database transformations from one store to another.  It can also sync from one to another or several stores.

Transporter
===========

About
-----

Transporter allows the user to configure a number of data adaptors as sources or sinks. These can be databases, files or other resources. Data is read from the sources, converted into a message format, and then send down to the sink where the message is converted into a writable format for its destination. The user can also create data transformations in JavaScript which can sit between the source and sink and manipulate or filter the message flow. 

Adaptors may be able to track changes as they happen in source data. This "tail" capability allows a Transporter to stay running and keep the sinks in sync.

Downloading Transporter
-----------------------

The latest binary releases are available from the [Github Repository](https://github.com/compose/transporter/releases/latest)

Adaptors
--------

Each adaptor has its own README page with details on configuration and capabilities.

* [elasticsearch](./adaptor/elasticsearch)
* [file](./adaptor/file)
* [mongodb](./adaptor/mongodb)
* [postgresql](./adaptor/postgres)
* [rethinkdb](./adaptor/rethinkdb)
* [transformer](./adaptor/transformer)

Commands
--------

### init

```
transporter init [source adaptor name] [sink adaptor name]
```

Generates a basic `transporter.yaml` and `pipeline.js` file in the current directory.

_Example_ 
```
$ transporter init mongodb elasticsearch
$ cat transporter.yaml
nodes:
  source:
    type: mongodb
    uri: ${MONGODB_URI} 
    # timeout: 30s
    # tail: false
    # ssl: false
    # cacerts: ["/path/to/cert.pem"]
    # wc: 1
    # fsync: false
    # bulk: false
  sink:
    type: elasticsearch
    uri: https://username:password@hostname:port
    # timeout: 10s # defaults to 30s
    # aws_access_key: XXX # used for signing requests to AWS Elasticsearch service
    # aws_access_secret: XXX # used for signing requests to AWS Elasticsearch service
$ cat pipeline.js`
Source({name:"source", namespace:"test./.*/"}).save({name:"sink", namespace:"test./.*/"})
$
```

Edit the `transporter.yaml` file to configure the source and sink nodes. The `pipeline.js` file will
also need to be edited to set the namespace.

### list

```
transporter list [--config transporterconfig.yaml]
```

List prints the currently configured nodes in the transporter.yaml file.

_Example_
```
Name                 Type            URI
sink                 elasticsearch   https://username:password@hostname:port
source               mongodb         
```

### About

`transporter about`

Lists all the adaptors currently available.

_Example_
```
elasticsearch - an elasticsearch sink adaptor
file - an adaptor that reads / writes files
mongodb - a mongodb adaptor that functions as both a source and a sink
postgres - a postgres adaptor that functions as both a source and a sink
rethinkdb - a rethinkdb adaptor that functions as both a source and a sink
transformer - an adaptor that transforms documents using a javascript function
```

### run

```
transporter run [--config transporterconfig.yaml] [-log.level "info"] <application.js>
```

Runs the pipeline script file which has its name given as the final parameter. 

### test

```
transporter test [--config transporterconfig.yaml] [-log.level "info"] <application.js>
```

Evaluates and connects the pipeline, sources and sinks. Establishes connections but does not run.
Prints out the state of connections at the end. Useful for debugging new configurations.

### eval

```
transporter test [--config transporterconfig.yaml] [-log.level "info"] 'JavaScript'
```

Runs the pipeline script found in the final parameter string. It needs to be a single string and properly escaped.


#### switches

`-config transporterconfig.yaml` - overrides the `transporter.yaml` default for the configuration file.

`-log.level "info"` - sets the logging level. Default is info; can be debug or error.

Building Transporter
--------------------

### Essentials

```
go build ./cmd/transporter/...
```

### Building guides

[macOS](https://github.com/compose/transporter/blob/master/READMEMACOS.md)
[Windows](https://github.com/compose/transporter/blob/master/READMEWINDOWS.md)
[Vagrant](https://github.com/compose/transporter/blob/master/READMEVAGRANT.md)

Transporter Resources
=====================

* [Transporter Wiki](https://github.com/compose/transporter/wiki)
* [Compose's articles](https://www.compose.io/articles/search/?s=transporter)

Contributing to Transporter
===========================

Want to help out with Transporter? Great! There are instructions to get you
started [here](CONTRIBUTING.md).

Licensing
=========
Transporter is licensed under the New BSD License. See LICENSE for full license text.

Support and Guarantees
======================
Compose does not provide support nor guarantee stability or functionality of this tool. Please take adequate caution when using Transporter to ensure that it's the right tool for the job. Transporter may not account for failure scenarios that could lead to unexpected behavior. Always take backups, always test in dev, and always feel free to submit a PR with enhancements, features, and bug fixes.
