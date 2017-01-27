package mongodb

import (
	"fmt"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	_ client.Reader = &Tailer{}
)

// Tailer implements the behavior defined by client.Tailer for interfacing with the MongoDB oplog.
type Tailer struct {
	reader client.Reader
	db     string

	oplogTimeout time.Duration
}

func newTailer(db string) client.Reader {
	return &Tailer{newReader(db), db, 5 * time.Second}
}

// Tail does the things
func (t *Tailer) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan message.Msg, error) {
		oplogTime := timeAsMongoTimestamp(time.Now())
		log.Infof("setting oplog start timestamp: %d", oplogTime)

		readFunc := t.reader.Read(filterFn)
		msgChan, err := readFunc(s, done)
		if err != nil {
			return nil, err
		}
		out := make(chan message.Msg)
		go func() {
			defer close(out)
			for msg := range msgChan {
				out <- msg
			}

			var (
				collection = s.(*Session).mgoSession.DB("local").C("oplog.rs")
				result     oplogDoc // hold the document
				query      = bson.M{"ts": bson.M{"$gte": oplogTime}}
				iter       = collection.Find(query).LogReplay().Sort("$natural").Tail(t.oplogTimeout)
			)
			defer iter.Close()

			for {
				log.With("db", t.db).Infof("tailing oplog with query %+v\n", query)
				select {
				case <-done:
					log.With("db", t.db).Infoln("tailing stopping...")
					return
				default:
					for iter.Next(&result) {
						if result.validOp() {
							db, c, _ := splitNamespace(result.Ns)
							if db != t.db {
								continue
							}
							if !filterFn(c) {
								continue
							}

							var (
								doc bson.M
								err error
								op  ops.Op
							)
							switch result.Op {
							case "i":
								op = ops.Insert
								doc = result.O
							case "d":
								op = ops.Delete
								doc = result.O
							case "u":
								op = ops.Update
								doc, err = t.getOriginalDoc(result.O2, c, s.(*Session).mgoSession)
								if err != nil {
									// errors aren't fatal here, but we need to send it down the pipe
									log.With("ns", result.Ns).Errorf("unable to getOriginalDoc, %s\n", err)
									// m.pipe.Err <- adaptor.NewError(adaptor.ERROR, m.path, fmt.Sprintf("tail MongoDB error (%s)", err.Error()), nil)
									continue
								}
							}

							msg := message.From(op, fmt.Sprintf("%s.%s", t.db, c), data.Data(doc)).(*message.Base)
							msg.TS = int64(result.Ts) >> 32

							out <- msg
							oplogTime = result.Ts
						}
						result = oplogDoc{}
					}
				}

				if iter.Timeout() {
					continue
				}
				if iter.Err() != nil {
					log.With("path", t.db).Errorf("error tailing oplog, %s\n", iter.Err())
					// return adaptor.NewError(adaptor.CRITICAL, m.path, fmt.Sprintf("MongoDB error (error reading collection %s)", iter.Err()), nil)
				}

				query = bson.M{"ts": bson.M{"$gte": oplogTime}}
				iter = collection.Find(query).LogReplay().Tail(t.oplogTimeout)
				time.Sleep(100 * time.Millisecond)
			}
		}()
		return out, nil
	}
}

// getOriginalDoc retrieves the original document from the database.
// transporter has no knowledge of update operations, all updates work as wholesale document replaces
func (t *Tailer) getOriginalDoc(doc bson.M, c string, s *mgo.Session) (result bson.M, err error) {
	id, exists := doc["_id"]
	if !exists {
		return result, fmt.Errorf("can't get _id from document")
	}

	err = s.DB(t.db).C(c).FindId(id).One(&result)
	if err != nil {
		err = fmt.Errorf("%s.%s %v %v", t.db, c, id, err)
	}
	return
}

// oplogDoc are representations of the mongodb oplog document
// detailed here, among other places.  http://www.kchodorow.com/blog/2010/10/12/replication-internals/
type oplogDoc struct {
	Ts bson.MongoTimestamp `bson:"ts"`
	H  int64               `bson:"h"`
	V  int                 `bson:"v"`
	Op string              `bson:"op"`
	Ns string              `bson:"ns"`
	O  bson.M              `bson:"o"`
	O2 bson.M              `bson:"o2"`
}

// validOp checks to see if we're an insert, delete, or update, otherwise the
// document is skilled.
// TODO: skip system collections
func (o *oplogDoc) validOp() bool {
	return o.Op == "i" || o.Op == "d" || o.Op == "u"
}

// splitNamespace split's a mongo namespace by the first '.' into a database and a collection
func splitNamespace(namespace string) (string, string, error) {
	fields := strings.SplitN(namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed mongo namespace")
	}
	return fields[0], fields[1], nil
}

func timeAsMongoTimestamp(t time.Time) bson.MongoTimestamp {
	return bson.MongoTimestamp(t.Unix() << 32)
}
