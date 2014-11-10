package node

import (
	"fmt"
	"strings"
	"time"

	"github.com/MongoHQ/transporter/pkg/message"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type MongoImpl struct {
	//the parent node
	// node *node.Node

	// pull these in from the node
	name      string
	uri       string
	namespace string
	role      NodeRole

	// save time by setting these once
	collection string
	database   string

	//
	//
	//
	//
	pipe Pipe

	//
	//
	//
	//
	//
	// mongo connection and options
	mongoSession *mgo.Session
	oplogTimeout time.Duration

	//
	//
	//

	restartable bool // this refers to being able to refresh the iterator, not to the restart based on session op
}

func NewMongoImpl(role NodeRole, name, kind, uri, namespace string) (*MongoImpl, error) {
	var (
		err error
	)

	m := &MongoImpl{
		restartable:  true,            // assume for that we're able to restart the process
		oplogTimeout: 5 * time.Second, // timeout the oplog iterator
		namespace:    namespace,
		role:         role,
	}

	m.database, m.collection, err = m.splitNamespace()
	if err != nil {
		return m, err
	}
	return m, nil
}

func (m *MongoImpl) Start(pipe Pipe) (err error) {
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

func (m *MongoImpl) writeMessage(msg *message.Msg) (err error) {
	collection := m.mongoSession.DB(m.database).C(m.collection)
	err = collection.Insert(msg.Document())
	if mgo.IsDup(err) {
		err = collection.Update(bson.M{"_id": msg.Id}, msg.Document())
	}
	return err
}

func (m *MongoImpl) catData() (err error) {

	var (
		collection = m.mongoSession.DB(m.database).C(m.collection)
		query      = bson.M{}
		result     bson.M // hold the document
	)

	iter := collection.Find(query).Sort("_id").Iter()

	for {
		for iter.Next(&result) {
			if stop := m.pipe.Stopping(); stop {
				return
			}

			// set up the message
			msg := message.NewMsg(message.Insert, m.namespace, result)

			m.pipe.Send(msg)
			result = bson.M{}
		}

		// we've exited the mongo read loop, lets figure out why
		// check here again if we've been asked to quit
		if stop := m.pipe.Stopping(); stop {
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

func (m *MongoImpl) tailData() (err error) {

	var (
		collection = m.mongoSession.DB("local").C("oplog.rs")
		result     OplogDoc // hold the document
		query      = bson.M{
			"ns": m.namespace,
		}
		iter = collection.Find(query).Sort("$natural").Tail(m.oplogTimeout)
	)

	for {
		for iter.Next(&result) {
			if stop := m.pipe.Stopping(); stop {
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
			result = OplogDoc{}
		}

		// we've exited the mongo read loop, lets figure out why
		// check here again if we've been asked to quit
		if stop := m.pipe.Stopping(); stop {
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

func (m *MongoImpl) Stop() error {
	m.pipe.Stop()
	return nil
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
		err = fmt.Errorf("%s %v %v", m.namespace, id, err)
	}
	return
}

/*
 * split a mongo namespace into a database and a collection
 */
func (m *MongoImpl) splitNamespace() (string, string, error) {
	fields := strings.SplitN(m.namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed mongo namespace.")
	}
	return fields[0], fields[1], nil
}

/*
 *
 * oplog documents are a specific structure
 *
 */
type OplogDoc struct {
	Ts bson.MongoTimestamp "ts"
	H  int64               "h"
	V  int                 "v"
	Op string              "op"
	Ns string              "ns"
	O  bson.M              "o"
	O2 bson.M              "o2"
}

func (o *OplogDoc) validOp() bool {
	// TODO skip system collections
	return o.Op == "i" || o.Op == "d" || o.Op == "u"
}
