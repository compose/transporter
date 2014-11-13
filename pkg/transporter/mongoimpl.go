package transporter

import (
	"fmt"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoImpl struct {
	// pull these in from the node
	uri  string
	role NodeRole

	// save time by setting these once
	collection string
	database   string

	//
	pipe pipe.Pipe

	//
	// mongo connection and options
	mongoSession *mgo.Session
	oplogTimeout time.Duration

	restartable bool // this refers to being able to refresh the iterator, not to the restart based on session op
}

func NewMongoImpl(namespace, uri string, role NodeRole, extra map[string]interface{}) (*MongoImpl, error) {
	var (
		err error
	)

	m := &MongoImpl{
		restartable:  true,            // assume for that we're able to restart the process
		oplogTimeout: 5 * time.Second, // timeout the oplog iterator
		uri:          uri,
		role:         role,
	}

	m.database, m.collection, err = m.splitNamespace(namespace)
	if err != nil {
		return m, err
	}
	return m, nil
}

func (m *MongoImpl) Start(pipe pipe.Pipe) (err error) {
	m.pipe = pipe
	defer func() {
		m.pipe.Stop()
	}()

	m.mongoSession, err = mgo.Dial(m.uri)
	if err != nil {
		m.pipe.Err <- err
		return err
	}

	// Source, cat and then tail the collection
	if m.role == SINK {
		return m.pipe.Listen(m.writeMessage)
	} else {
		err = m.catData()
		if err != nil {
			m.pipe.Err <- err
			return err
		}

		// replay the oplog
		err = m.tailData()
		if err != nil {
			m.pipe.Err <- err
			return err
		}
	}
	return
}

func (m *MongoImpl) Stop() error {
	m.pipe.Stop()
	return nil
}

func (m *MongoImpl) writeMessage(msg *message.Msg) (*message.Msg, error) {
	collection := m.mongoSession.DB(m.database).C(m.collection)
	err := collection.Insert(msg.Document())
	if mgo.IsDup(err) {
		err = collection.Update(bson.M{"_id": msg.Id}, msg.Document())
	}
	return msg, err
}

/*
 * pull down the original connection
 */
func (m *MongoImpl) catData() (err error) {

	var (
		collection = m.mongoSession.DB(m.database).C(m.collection)
		query      = bson.M{}
		result     bson.M // hold the document
	)

	iter := collection.Find(query).Sort("_id").Iter()

	for {
		for iter.Next(&result) {
			if stop := m.pipe.Stopped; stop {
				return
			}

			// set up the message
			msg := message.NewMsg(message.Insert, m.getNamespace(), result)

			m.pipe.Send(msg)
			result = bson.M{}
		}

		// we've exited the mongo read loop, lets figure out why
		// check here again if we've been asked to quit
		if stop := m.pipe.Stopped; stop {
			return
		}

		if iter.Err() != nil && m.restartable {
			fmt.Printf("got err reading collection. reissuing query. %v\n", iter.Err())
			time.Sleep(1 * time.Second)
			iter = collection.Find(query).Sort("_id").Iter()
			continue
		}

		return
	}
}

/*
 * tail the oplog
 */
func (m *MongoImpl) tailData() (err error) {

	var (
		collection = m.mongoSession.DB("local").C("oplog.rs")
		result     oplogDoc // hold the document
		query      = bson.M{
			"ns": m.getNamespace(),
		}
		iter = collection.Find(query).Sort("$natural").Tail(m.oplogTimeout)
	)

	for {
		for iter.Next(&result) {
			if stop := m.pipe.Stopped; stop {
				return
			}
			if result.validOp() {

				msg := message.NewMsg(message.OpTypeFromString(result.Op), result.Ns, nil)
				msg.Timestamp = int64(result.Ts) >> 32

				switch result.Op {
				case "i":
					msg.SetDocument(result.O)
				case "d":
					msg.SetDocument(result.O)
				case "u":
					doc, err := m.getOriginalDoc(result.O2)
					if err != nil {
						m.pipe.Err <- err
						return err
					}
					msg.SetDocument(doc)
				}

				m.pipe.Send(msg)
			}
			result = oplogDoc{}
		}

		// we've exited the mongo read loop, lets figure out why
		// check here again if we've been asked to quit
		if stop := m.pipe.Stopped; stop {
			return
		}
		if iter.Timeout() {
			continue
		}
		if iter.Err() != nil {
			return fmt.Errorf("got err reading collection. %v\n", iter.Err())
		}

		iter = collection.Find(query).Sort("$natural").Tail(m.oplogTimeout)
	}
}

/*
 * update operations need us to get the original document from mongo
 */
func (m *MongoImpl) getOriginalDoc(doc bson.M) (result bson.M, err error) {
	id, exists := doc["_id"]
	if !exists {
		return result, fmt.Errorf("Can't get _id from document")
	}

	err = m.mongoSession.DB(m.database).C(m.collection).FindId(id).One(&result)
	if err != nil {
		err = fmt.Errorf("%s %v %v", m.getNamespace(), id, err)
	}
	return
}

func (m *MongoImpl) getNamespace() string {
	return strings.Join([]string{m.database, m.collection}, ".")
}

/*
 * split a mongo namespace into a database and a collection
 */
func (m *MongoImpl) splitNamespace(namespace string) (string, string, error) {
	fields := strings.SplitN(namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed mongo namespace.")
	}
	return fields[0], fields[1], nil
}

/*
 * oplog documents are a specific structure
 */
type oplogDoc struct {
	Ts bson.MongoTimestamp `bson:"ts"`
	H  int64               `bson:"h"`
	V  int                 `bson:"v"`
	Op string              `bson:"op"`
	Ns string              `bson:"ns"`
	O  bson.M              `bson:"o"`
	O2 bson.M              `bson:"o2"`
}

func (o *oplogDoc) validOp() bool {
	// TODO skip system collections
	return o.Op == "i" || o.Op == "d" || o.Op == "u"
}
