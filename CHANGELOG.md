## v0.4.0 [2017-08-15]

### Features
- updated to Go 1.8
- MongoDB Read Preferences [#393](https://github.com/compose/transporter/pull/393), thanks @SamBartrum!
- ability to support [Continuous Change Data Capture](https://github.com/compose/transporter/blob/master/DESIGN.md#continuous-change-data-capture) as a Beta feature, see *BETA Feature* section on [README](https://github.com/compose/transporter#about)

### Bugfixes
- when using MongoDB as a source with tailing enabled and namespace filtering, it was possible for documents
from others collections to be sent down the pipeline, fixed via [#386](https://github.com/compose/transporter/pull/386)
- if transporter lost connection to MongoDB while tailing the oplog, the connection never successfully reconnected, fixed via [#398](https://github.com/compose/transporter/pull/398)

## v0.3.1 [2017-03-24]

### Features
- added `js` alias for `goja` transform function [#335](https://github.com/compose/transporter/pull/335) 
- `init` command will now prompt user when `pipeline.js` file already exists [#336](https://github.com/compose/transporter/pull/336)

### Bugfixes
- fixed mongodb, rabbitmq, and rethinkdb adaptors from not trying to read from a file when provided in the `ca_certs` field [#334](https://github.com/compose/transporter/pull/334)

## v0.3.0 [2017-03-21]

### Breaking changes

***PLEASE READ***

Transporter no longer requires a YAML file. All configuration is in the JS file where nodes are defined and a new DSL has been developed. Run the `init` command to see the new changes.

- if using transporter as a library, all packages have been moved out of `pkg` to the top-level
- `eval` command removed
- `list` command removed
- the `namespace` parameter now only expects a single part (the regexp filter), all adaptors have been updated to pull the "database name" from the provided URI

### Features
- NEW RabbitMQ adaptor [#298](https://github.com/compose/transporter/pull/298)
- MongoDB adaptor supports per collection query filter when needing to copy only a subset of data [#301](https://github.com/compose/transporter/pull/301)
- [goja](https://github.com/dop251/goja) added as an option for the JavaScript VM in transformers [#294](https://github.com/compose/transporter/pull/294)
- NEW [native functions](https://github.com/compose/transporter#native-functions)

### Bugfixes

## v0.2.2 [2017-03-20]

### Features

### Bugfixes
- attempted fix for mejson.S conversion to json column in postgres adaptor [#314](https://github.com/compose/transporter/issues/314)

## v0.2.1 [2017-03-07]

### Features
- added RethinkDB -> PostgreSQL integration test

### Bugfixes
- fixed connection leak in PostgreSQL client

## v0.2.0 [2017-02-28]

### Breaking changes
- The etcd adaptor was removed

### Features
- Integration tests are run weekly for the following pipelines:
  1. MongoDB -> MongoDB
  2. MongoDB -> Elasticsearch
  3. MongoDB -> RethinkDB
- RethinkDB SSL support added in [#268](https://github.com/compose/transporter/pull/268)
- RethinkDB performs bulk inserts now, [#276](https://github.com/compose/transporter/pull/276)
- `transporter init [source] [sink]` command added in [#279](https://github.com/compose/transporter/pull/279)

### Bugfixes
- MongoDB adaptor could cause the pipeline to stop due to a concurrent flush operation, fixed via [#271](https://github.com/compose/transporter/pull/271)
- When being used as a library, several goroutines were leaking after the pipeline had stopped. Reported by @cognusion in [#265](https://github.com/compose/transporter/issues/265) and addressed via [#268](https://github.com/compose/transporter/pull/268) and [#280](https://github.com/compose/transporter/pull/280)

## v0.1.3 [2017-02-09]

### Breaking changes
- namespace processing no longer expects their the be a "db" portion (i.e. "database.collection")
but an attempt to maintain backwards compatibility is still there for the time being.
[#258](https://github.com/compose/transporter/pull/258)

### Bugfixes
- [#261](https://github.com/compose/transporter/pull/261): return a nil message to get skipped in the pipeline

## v0.1.2 [2017-01-27]

This release is primarily aimed at getting the MongoDB and Elasticsearch adaptors into a
stable/reliable state.

### Breaking changes
- MongoDB adaptor SSL configuration is now defined as:
```yaml
nodes:
  localmongo:
    type: mongodb
    ssl: true
    cacerts: ["/path/to/cert.pem"] # optional
```

### Bugfixes
- [#211](https://github.com/compose/transporter/pull/211): defer bulk channel init for mongo node reuse
- [#213](https://github.com/compose/transporter/pull/213): track mongodb \_id field so we can attempt to reissue queries
- [#233](https://github.com/compose/transporter/pull/233): update elasticsearch adaptor with better support
for multiple versions of elasticsearch as well as better performance with bulk indexing for most versions.
Addresses [#209](https://github.com/compose/transporter/issues/209), [#222](https://github.com/compose/transporter/issues/222),
[#167](https://github.com/compose/transporter/issues/167) and [#159](https://github.com/compose/transporter/issues/159).
- properly detect oplog access when attempting to use the `tail` option on the MongoDB adaptor.

## v0.1.1 [2015-08-27]

This release contains the first step to getting savable state into adaptors for the ability to resume.

### Features
- [#116](https://github.com/compose/transporter/pull/116): Adaptor state phase 1, begins to address [#33](https://github.com/compose/transporter/issues/33)

### Bugfixes
- [#124](https://github.com/compose/transporter/issues/124): Update elasticsearch adaptor


## v0.1.0 [2015-08-06]

This release contains several breaking changes but is a first step towards a stable API/DSL.

### Features
- [#95](https://github.com/compose/transporter/pull/95): add a Noop message type, and the ability to skip messages, fixes [#93](https://github.com/compose/transporter/issues/93)
- [#100](https://github.com/compose/transporter/pull/100): replace environment variables, fixes [#88](https://github.com/compose/transporter/issues/88)
- [#101](https://github.com/compose/transporter/pull/101): (Phase 1) Multi namespace support, addresses [#78](https://github.com/compose/transporter/issues/78) and [#23](https://github.com/compose/transporter/issues/23)

### Bugfixes
- [#94](https://github.com/compose/transporter/pull/94): jsonlog, fixes [#92](Comment doesnt reflect jsonlog output)
- [#98](https://github.com/compose/transporter/pull/98): Fixes influx_test.js, thanks @ExNexu!
- [#97](https://github.com/compose/transporter/pull/97): throw transformer initialization errors early, fixes [#96](https://github.com/compose/transporter/issues/96)


## v0.0.4 [2015-07-13]

This release contains several breaking changes:
- Transporter now uses [godep](https://github.com/tools/godep) for vendoring depedencies.
- transformer functions now receive a document in the following format:
```
{
  "op": "insert/update/delete",
  "data": "", // what use to be passed in the doc passed to the transformer function
  "ts": 1436794521, // the number of seconds elapsed since January 1, 1970 UTC
}
```

The same document must be returned from the function so any modifications must happen to `doc["data"]`.

***NOTE***: Deletes are now passed to the transformer functions so this needs to be accounted for.

### Features
- [#46](https://github.com/compose/transporter/pull/46): add a JsonLog.
- [#64](https://github.com/compose/transporter/pull/64): New Source Adapter: Implements RethinkDB as a source of documents. Thanks @alindeman!
- [#68](https://github.com/compose/transporter/pull/68): Deletes from Mongo write adaptor.
- [#72](https://github.com/compose/transporter/pull/72): Allows RethinkDB timeout to be configured.
- [#83](https://github.com/compose/transporter/pull/83): RethinkDB: Changes for v1 RethinkDB driver.
- [#86](https://github.com/compose/transporter/pull/86): Use godep to vendor required libraries.
- [#87](https://github.com/compose/transporter/pull/87): add SSL support for MongoDB, bump mgo.
- [#90](https://github.com/compose/transporter/pull/90): ***NOTE*** BREAKING CHANGE: send more info in doc to transformers.

### Bugfixes
- [#47](https://github.com/compose/transporter/pull/47): A few readme grammar fixes. Thanks @mm-!
- [#49](https://github.com/compose/transporter/pull/49): fixes [#48](https://github.com/compose/transporter/issues/48)
- [#51](https://github.com/compose/transporter/pull/51): fixes [#50](https://github.com/compose/transporter/issues/50)
- [#54](https://github.com/compose/transporter/pull/54): import rethinkdb from gopkg.in. it's still v0, so this will change still.
- [#55](https://github.com/compose/transporter/pull/55): remove influx until the state of the repository settles or we get proper vendoring in place.
- [#63](https://github.com/compose/transporter/pull/63): use a smaller buffer for mongo adaptor.
- [#65](https://github.com/compose/transporter/pull/65): Extracts only the relevant pieces of the version string. Thanks @alindeman!
- [#85](https://github.com/compose/transporter/pull/85): ensure we're at version >=2.0 of rethink for this driver.


## v0.0.3 [2015-01-14]

### Features
- [#36](https://github.com/compose/transporter/pull/36): Simplifies the config.yaml and node configuration.
- [#38](https://github.com/compose/transporter/pull/38): add a quick benchmark to transformOne

### Bugfixes
- [#37](https://github.com/compose/transporter/pull/37): fixes [#29](https://github.com/compose/transporter/issues/29)
- [#42](https://github.com/compose/transporter/pull/42): fixes [#41](https://github.com/compose/transporter/issues/41)
- [#44](https://github.com/compose/transporter/pull/44): fix a logical error. check for transporter first.
- [#45](https://github.com/compose/transporter/pull/45): add a bulk writer, and writeconcern options for mongo.


## v0.0.2 [2014-12-29]

### Features
- [#22](https://github.com/compose/transporter/pull/22): adaptor constructor refactor, new `transporter about` command.
- [#26](https://github.com/compose/transporter/pull/26): adds the name and description to about.
- [#27](https://github.com/compose/transporter/pull/27): Instructions for building Transporter on Windows.
- [#31](https://github.com/compose/transporter/pull/31): add some more information in errors that we get in the javascript.
- [#32](https://github.com/compose/transporter/pull/32): Fix typo in registry comments for doc gen.

### Bugfixes
- [#7](https://github.com/compose/transporter/pull/7): Make list informative, stop arg panics.
- [#9](https://github.com/compose/transporter/pull/9): beginners guide for os x. Thanks @sberryman!
- [#10](https://github.com/compose/transporter/pull/10): Pretty print the list.
- [#11](https://github.com/compose/transporter/pull/11): fix the config.yaml example to show the proper interval syntax.
- [#13](https://github.com/compose/transporter/pull/15): makes the api in the config.yaml optional.
- [#18](https://github.com/compose/transporter/pull/18): check that errors aren't nil.
- [#20](https://github.com/compose/transporter/pull/20): log any adaptor.ERROR or adaptor.CRITICAL.


## v0.0.1 [2014-12-12]

### Release Notes

This is the initial release of Transporter.
