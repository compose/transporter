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
