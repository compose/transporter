package adaptor

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	MONGO_BUFFER_SIZE int = 1e6
	MONGO_BUFFER_LEN  int = 1e5
)

// Mongodb is an adaptor to read / write to mongodb.
// it works as a source by copying files, and then optionally tailing the oplog
type Mongodb struct {
	// pull these in from the node
	uri   string
	tail  bool // run the tail oplog
	debug bool

	// save time by setting these once
	collection string
	database   string

	oplogTime bson.MongoTimestamp

	//
	pipe *pipe.Pipe
	path string

	// mongo connection and options
	mongoSession *mgo.Session
	oplogTimeout time.Duration

	// a buffer to hold documents
	buffLock         sync.Mutex
	opsBuffer        []interface{}
	opsBufferSize    int
	bulkWriteChannel chan interface{}
	bulkQuitChannel  chan chan bool
	bulk             bool

	restartable bool // this refers to being able to refresh the iterator, not to the restart based on session op
}

// NewMongodb creates a new Mongodb adaptor
func NewMongodb(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
	var (
		conf MongodbConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	if conf.URI == "" || conf.Namespace == "" {
		return nil, fmt.Errorf("both uri and namespace required, but missing ")
	}

	if conf.Debug {
		fmt.Printf("Mongo Config %+v\n", conf)
	}

	m := &Mongodb{
		restartable:      true,            // assume for that we're able to restart the process
		oplogTimeout:     5 * time.Second, // timeout the oplog iterator
		pipe:             p,
		uri:              conf.URI,
		tail:             conf.Tail,
		debug:            conf.Debug,
		path:             path,
		opsBuffer:        make([]interface{}, 0, MONGO_BUFFER_SIZE),
		bulkWriteChannel: make(chan interface{}),
		bulkQuitChannel:  make(chan chan bool),
		bulk:             conf.Bulk,
	}

	m.database, m.collection, err = m.splitNamespace(conf.Namespace)
	if err != nil {
		return m, err
	}

	m.mongoSession, err = mgo.Dial(m.uri)

	if err != nil {
		return m, err
	}

	// set some options on the session
	m.mongoSession.EnsureSafe(&mgo.Safe{W: conf.Wc, FSync: conf.FSync})
	m.mongoSession.SetBatch(1000)
	m.mongoSession.SetPrefetch(0.5)

	return m, nil
}

// Start the adaptor as a source
func (m *Mongodb) Start() (err error) {
	defer func() {
		m.pipe.Stop()
	}()

	m.oplogTime = nowAsMongoTimestamp()
	if m.debug {
		fmt.Printf("setting start timestamp: %d\n", m.oplogTime)
	}

	err = m.catData()
	if err != nil {
		m.pipe.Err <- err
		return err
	}
	if m.tail {
		// replay the oplog
		err = m.tailData()
		if err != nil {
			m.pipe.Err <- err
			return err
		}
	}

	return
}

// Listen starts the pipe's listener
func (m *Mongodb) Listen() (err error) {
	defer func() {
		m.pipe.Stop()
	}()

	if m.bulk {
		go m.bulkWriter()
	}
	return m.pipe.Listen(m.writeMessage)
}

// Stop the adaptor
func (m *Mongodb) Stop() error {
	m.pipe.Stop()

	// if we're bulk writing, ask our writer to exit here
	if m.bulk {
		q := make(chan bool)
		m.bulkQuitChannel <- q
		<-q
	}

	return nil
}

// writeMessage writes one message to the destination mongo, or sends an error down the pipe
// TODO this can be cleaned up.  I'm not sure whether this should pipe the error, or whether the
//   caller should pipe the error
func (m *Mongodb) writeMessage(msg *message.Msg) (*message.Msg, error) {
	collection := m.mongoSession.DB(m.database).C(m.collection)

	if !msg.IsMap() {
		m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (document must be a bson document, got %T instead)", msg.Data), msg.Data)
		return msg, nil
	}

	doc := msg.Map()
	if m.bulk {
		m.bulkWriteChannel <- doc
	} else {
		err := collection.Insert(doc)
		if mgo.IsDup(err) {
			err = collection.Update(bson.M{"_id": doc["_id"]}, doc)
		}
		if err != nil {
			m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (%s)", err.Error()), msg.Data)
		}
	}

	return msg, nil
}

func (m *Mongodb) bulkWriter() {

	for {
		select {
		case doc := <-m.bulkWriteChannel:
			sz, err := docSize(doc)
			if err != nil {
				m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (%s)", err.Error()), doc)
				break
			}

			if ((sz + m.opsBufferSize) > MONGO_BUFFER_SIZE) || (len(m.opsBuffer) == MONGO_BUFFER_LEN) {
				m.writeBuffer() // send it off to be inserted
			}

			m.buffLock.Lock()
			m.opsBuffer = append(m.opsBuffer, doc)
			m.opsBufferSize += sz
			m.buffLock.Unlock()
		case <-time.After(2 * time.Second):
			m.writeBuffer()
		case q := <-m.bulkQuitChannel:
			m.writeBuffer()
			q <- true
		}
	}
}

func (m *Mongodb) writeBuffer() {
	m.buffLock.Lock()
	defer m.buffLock.Unlock()
	collection := m.mongoSession.DB(m.database).C(m.collection)

	err := collection.Insert(m.opsBuffer...)

	if err != nil {
		if mgo.IsDup(err) {
			err = nil
			for _, op := range m.opsBuffer {
				e := collection.Insert(op)
				if mgo.IsDup(e) {
					doc, ok := op.(map[string]interface{})
					if !ok {
						m.pipe.Err <- NewError(ERROR, m.path, "mongodb error (Cannot cast document to bson)", op)
					}

					e = collection.Update(bson.M{"_id": doc["_id"]}, doc)
				}
				if e != nil {
					m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (%s)", e.Error()), op)
				}
			}
		} else {
			m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (%s)", err.Error()), m.opsBuffer[0])
		}
	}

	m.opsBuffer = make([]interface{}, 0, MONGO_BUFFER_SIZE)
	m.opsBufferSize = 0
}

// catdata pulls down the original collection
func (m *Mongodb) catData() (err error) {
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
			msg := message.NewMsg(message.Insert, result)

			m.pipe.Send(msg)
			result = bson.M{}
		}

		// we've exited the mongo read loop, lets figure out why
		// check here again if we've been asked to quit
		if stop := m.pipe.Stopped; stop {
			return
		}

		if iter.Err() != nil && m.restartable {
			fmt.Printf("got err reading collection. reissuing query %v\n", iter.Err())
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
func (m *Mongodb) tailData() (err error) {

	var (
		collection = m.mongoSession.DB("local").C("oplog.rs")
		result     oplogDoc // hold the document
		query      = bson.M{
			"ts": bson.M{"$gte": m.oplogTime},
			"ns": m.getNamespace(),
		}

		iter = collection.Find(query).LogReplay().Sort("$natural").Tail(m.oplogTimeout)
	)

	for {
		for iter.Next(&result) {
			if stop := m.pipe.Stopped; stop {
				return
			}
			if result.validOp() {

				var doc bson.M
				switch result.Op {
				case "i":
					doc = result.O
				case "d":
					doc = result.O
				case "u":
					doc, err = m.getOriginalDoc(result.O2)
					if err != nil { // errors aren't fatal here, but we need to send it down the pipe
						m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("Mongodb error (%s)", err.Error()), nil)
						continue
					}
				default:
					m.pipe.Err <- NewError(ERROR, m.path, "Mongodb error (unknown op type)", nil)
					continue
				}

				msg := message.NewMsg(message.OpTypeFromString(result.Op), doc)
				msg.Timestamp = int64(result.Ts) >> 32

				m.oplogTime = result.Ts
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
			return NewError(CRITICAL, m.path, fmt.Sprintf("Mongodb error (error reading collection %s)", iter.Err()), nil)
		}

		// query will change,
		query = bson.M{
			"ts": bson.M{"$gte": m.oplogTime},
			"ns": m.getNamespace(),
		}
		iter = collection.Find(query).LogReplay().Tail(m.oplogTimeout)
	}
}

// getOriginalDoc retrieves the original document from the database.  transport has no knowledge of update operations, all updates
// work as wholesale document replaces
func (m *Mongodb) getOriginalDoc(doc bson.M) (result bson.M, err error) {
	id, exists := doc["_id"]
	if !exists {
		return result, fmt.Errorf("can't get _id from document")
	}

	err = m.mongoSession.DB(m.database).C(m.collection).FindId(id).One(&result)
	if err != nil {
		err = fmt.Errorf("%s %v %v", m.getNamespace(), id, err)
	}
	return
}

func (m *Mongodb) getNamespace() string {
	return strings.Join([]string{m.database, m.collection}, ".")
}

// splitNamespace split's a mongo namespace by the first '.' into a database and a collection
func (m *Mongodb) splitNamespace(namespace string) (string, string, error) {
	fields := strings.SplitN(namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed mongo namespace")
	}
	return fields[0], fields[1], nil
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

// MongodbConfig provides configuration options for a mongodb adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type MongodbConfig struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form mongodb://user:password@host.com:27017/auth_database"`
	Namespace string `json:"namespace" doc:"mongo namespace to read/write"`
	Debug     bool   `json:"debug" doc:"display debug information"`
	Tail      bool   `json:"tail" doc:"if tail is true, then the mongodb source will tail the oplog after copying the namespace"`
	Wc        int    `json:"wc" doc:"The write concern to use for writes, Int, indicating the minimum number of servers to write to before returning success/failure"`
	FSync     bool   `json:"fsync" doc:"When writing, should we flush to disk before returning success"`
	Bulk      bool   `json:"bulk" doc:"use a buffer to bulk insert documents"`
}

func nowAsMongoTimestamp() bson.MongoTimestamp {
	return bson.MongoTimestamp(time.Now().Unix() << 32)
}

func newMongoTimestamp(s, i int) bson.MongoTimestamp {
	return bson.MongoTimestamp(int64(s)<<32 + int64(i))
}

// find the size of a document in bytes
func docSize(ops interface{}) (int, error) {
	b, err := bson.Marshal(ops)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}
