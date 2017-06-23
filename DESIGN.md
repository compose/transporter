# Design

This document contains both a high level overview of the system and then introduces
proposed changes to enable the system to make certain guarantees about message delivery.
It will then detail changes to enable the system to allow adaptors to "resume" processing
after either a graceful or interrupted restart of the system.

## Adaptors

We want to support various types of systems pertaining to data storage/transport. Most systems
will support the ability for both reading/writing, however, the system allows for one or the 
other implementation. Adaptors are somewhat analogous to a producer/consumer model and can be 
depicted below (R = reader, W = writer, M = message):

```
     +-----------+
     |           | -> W^1^
R -> |     M     | -> W^2^
     |           | -> W^3^
     +-----------+
```

## Messages

Because data structure differs from system to system, transporter attempts to be as agnostic as possible
to any particulars... with the exception of MongoDB due to its [extended JSON](https://docs.mongodb.com/manual/reference/mongodb-extended-json/). Every message flowing through the system takes the following form:

```go
type Message struct {
  Op:        int // Insert/Delete/Update/...
  Timestamp: int64 // internal timestamp
  Namespace: string // generic representation of a schema name (i.e. collection/table/queue)
  Data:      map[string]interface{}
}
```

Messages flow through the system ***one at a time*** which means most writer implementations 
will favor a bulk mechanism for performance.

## Namespace Filtering

Since data is typically segregated into different "buckets" within the underlying system, transporter
uses a `namespace` concept to enable filtering of the data at the "bucket" level. A `namespace` is 
configured by providing a regular expression as defined by the [regexp pkg](https://golang.org/pkg/regexp/).
By default, if no `namespace` is provided, the system will use a "catch all" filter in the form of `.*`.
As messages flow through the system, its originating namespace is matched against the configured 
`namespace` to determine whether the message will continue to the next hop in the system.

The goal of namespace filtering is to be able to define a single reader and filter which segments
are sent to 1 or more writers.

## Data Transformation

Transporter differs from other systems (some, not all) by being able to insert transformation functions
into the data flow pipeline. By doing so, it allows users to manipulate the data structure through code
to ensure the system receiving the data can be handled properly. Below is an example diagram depicting
the use of a transformation function (F = transformation function:

```
     +-----------+
     |           | -> F^1^ -> F^2^ -> W^1^
R -> |     M     | -> W^2^
     |           | -> F^3^ -> W^3^
     +-----------+
```

These function not only allow for manipulating the data structure being sent to do downstream writer 
but can also make a decision of whether or not to "drop" messages such that they never reach the writer.

## Continuous Change Data Capture

Many of today's systems support the ability to not only scan/copy data at a point in time but also
allow for continuously reading changes from the system as they occur in near real-time. It is the goal
of transporter to support continuous change data capture wherever possible based on the underlying 
systems capabilities. 

## Message Guarantees

The system will support an ***at least once*** delivery guarantee wherein the possibility of the same
message being sent more than once exists. 

The requirements for each component in the system could be broken down as follows:

### readers
- need to be able to attach additional information about each message and namespace:
  - collection XYZ has completed the "copy" portion and is in "sync" phase but collection ABC never 
    completed the "copy" phase).
  - mongodb oplog time of last message was 123456
- need to be provided with the data for the last message it had sent down the pipeline

### writers
- need to be able to acknowledge processing of messages both individually and in bulk
- each writer must have its own message "offset" given situations where one writer only receives messages
  1/2/4 and another writer receives messages 1/3/5/7 due to namespace or function filtering

### functions
- need to be able to acknowledge a message was processed for cases where the function resulted in the 
  message not being sent to the underlying writer (i.e. skipped)

In order to facilitate this, the reader will have an append only commit log. When a message 
is sent down the pipeline, it will be appended to the commit log. Each writer will have its own offset in the 
offset log (similar to a consumer group in kafka) wherein an acknowledgement of each message can be provided 
after a successful write to the underlying source or if the message was skipped, the commit id will be 
appended to the offset log. _If_ the writer uses a bulk mechanism, any errors returned from the bulk operation 
should cause the system to stop. The consmer offsets will be written to the log at a configurable interval 
(default of 1s) wherein a forceful termination of the system will only result in a processed message's offset 
not being written to disk equal to the total amount of time between the interval.

### Normal Operations

Example of a bulk writer

```
X     = message
A/B/C = key (equal to the namespace)
c0    = consumer 0 offset
c1    = consumer 1 offset
```

commit log
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
msg| X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X |   |
key| A | A | B | B | A | A | B | B | C | A | C | B | A | A | C | B |   |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
id   0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16
```

offset log
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+
key|c0 |c0 |c0 |c1 |c0 |c1 |c0 |c1 |c0 |c1 |c0 |   |   |   |
val| 1 | 4 | 6 | 4 | 4 | 5 | 7 | 7 | 10| 13| 12|   |   |   |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+
```

In the above scenario, the following would represent the current state of the system:

```
commit log = position 15
c0 offset  = position 12
c1 offset  = position 13
```

Messages 13-15 have either not been committed to the underlying system or are in the process 
of being committed and awaiting a response for c0 and messages 14-15 for c1.

Should the system be forcefully terminated and restarted, messages 13-15 will be redelivered to 
c0 and the system will wait until its offset is at position 15 to ensure message delivery. The same 
process will be performed for c1 with redelivery of messages 14-15 and wait until its offset is at 
position 15.

If the system performs a "clean" shutdown, it provides a 30 second window to allow all writers to commit
any messages to the underlying system.

Assuming a clean resume, the state of the system would then be:

commit log
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
msg| X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X |   |
key| A | A | B | B | A | A | B | B | C | A | C | B | A | A | C | B |   |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
id   0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16
```

offset log
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+
key|c0 |c0 |c0 |c1 |c0 |c1 |c0 |c1 |c0 |c1 |c0 |c0 |c1 |   |
val| 1 | 4 | 6 | 4 | 4 | 5 | 7 | 7 | 10| 13| 12| 15| 15|   |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+
```

### Message Failures

Example of a bulk writer that receives an error response during a commit:

commit log
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
msg| X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X |   |
key| A | A | A | A | A | A | A | A | A | A | A | A | A | A | A | A |   |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
id   0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16
```

offset log
```
   +---+---+---+---+---+---+---+---+---+---+
key|c0 |c0 |c0 |c0 |c0 |c0 |   |   |   |   |
val| 1 | 3 | 4 | 5 | 7 | 8 |   |   |   |   |
   +---+---+---+---+---+---+---+---+---+---+
```

Writer attempts to commit messages 9-15 but receives an error response.

commit log
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
msg| X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X |   |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
id   0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16
```

offset log
```
   +---+---+---+---+---+---+---+---+---+---+
key|c0 |c0 |c0 |c0 |c0 |c0 |   |   |   |   |
val| 1 | 3 | 4 | 5 | 7 | 8 |   |   |   |   |
   +---+---+---+---+---+---+---+---+---+---+
```

In the above scenario, c0 offset will still be 8 and the messages that were part of the errored 
commit (9-15) will remain in the commit log. Upon restart (whether automatic or manual), the system will 
redeliver messages 9-15 *ONE AT A TIME* to c0 and wait for acknowledgement before continuing. This means 
that all bulk operations will occur with individual messages. 

If the messages are successfully written to the underlying source, the system will then resume operations as 
normal by providing the last offset message to the reader. 

If an error occurs for a commit during the resume phase, the system will shutdown leaving c0 at the last 
committed offset. In the above scenario, let's say messages 9-13 were successfully written to the underlying 
source but message 14 resulted in a error, then c0 offset would be 13. 

If messages 14-15 continue to cause the writer to error and system to shutdown, operator intervention will 
be required to properly handle the messages. 

The following capabilities will be made availabe to the operator:

- process each message one at a time
- view messages in the commit log from offset X to Y
- mark a message as acknowledged without sending to the writer

Once all consumer offsets are equal to the last offset in the commit log, the system will be able to resume
reading new messages and sending them down the pipeline.

### Log Compaction

Every message in the system will remain in the commit log based on a configurable size with a default
of 1GB. Once the configured size is reached, a new log will be created and become the active log.
A process will handle compacting the commit log up to the earliest consumer offset while also
retaining at least one message for each key (i.e. namespace) in the log. 

Only non-active log segments are eligible for compaction. This will prevent any large stop the world pauses in the 
system on the active segment and will allow the system to create a full view of each message associated with a given 
key (i.e. namespace) over the entire course of time.

The offset log will also be compacted continually such that every consumer will have at least 1 offset in 
the log for each unique key.

commit log before compaction
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
msg| X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X |
key| A | A | B | B | A | A | B | B | C | A | C | B | A | A | C | B | A |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
id   0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16
```

active commit log before compaction
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
msg| X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X |
key| A | A | B | B | A | A | B | B | C | A | C |   |   |   |   |   |   |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
id   17  18  19  20  21  22  23  24  25  26  27  28  29  30  31  32  33
```

offset log
```
   +---+---+---+---+---+---+---+---+---+---+
key|c0A|c0A|c0B|c0C|c0A|c0B|c0C|c0A|c0B|c0C|
val| 1 | 8 | 1 | 3 | 13| 12| 9 | 26| 23| 27|
   +---+---+---+---+---+---+---+---+---+---+
```

old commit log after compaction
```
   +---+---+---+
msg| X | X | X |
key| C | B | A |
   +---+---+---+
id   14  15  16  
```

active commit log after compaction
```
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
msg| X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X | X |
key| A | A | B | B | A | A | B | B | C | A | C |   |   |   |   |   |   |
   +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
id   17  18  19  20  21  22  23  24  25  26  27  28  29  30  31  32  33
```

offset log after compaction
```
   +---+---+---+---+---+---+---+---+---+---+
key|c0A|c0B|c0C|   |   |   |   |   |   |   |
val| 26| 23| 27|   |   |   |   |   |   |   |
   +---+---+---+---+---+---+---+---+---+---+
```

## Message Format

### Message
```
key length  - 4 bytes
key         - K bytes
data length - 4 bytes
data        - D bytes
```

### Log
```
offset         - 8 bytes
message length - 4 bytes
timestamp      - 8 bytes
mode           - 1 byte
key length     - 4 bytes
key            - K bytes
data length    - 4 bytes
data           - D bytes
```

```Go
type LogEntry struct {
	Key        []byte
	Value      []byte
	Timestamp  uint64
	Mode       Mode
}

type Mode int

const (
	Copy Mode = iota
	Sync
	Complete
)
```



