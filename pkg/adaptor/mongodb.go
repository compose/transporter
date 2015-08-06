package adaptor

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"regexp"
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
	MONGO_BUFFER_LEN  int = 5e5
)

// Mongodb is an adaptor to read / write to mongodb.
// it works as a source by copying files, and then optionally tailing the oplog
type Mongodb struct {
	// pull these in from the node
	uri   string
	tail  bool // run the tail oplog
	debug bool

	// save time by setting these once
	collectionMatch *regexp.Regexp
	database        string

	oplogTime bson.MongoTimestamp

	//
	pipe *pipe.Pipe
	path string

	// mongo connection and options
	mongoSession *mgo.Session
	oplogTimeout time.Duration

	// a buffer to hold documents
	buffLock         sync.Mutex
	opsBufferCount   int
	opsBuffer        map[string][]interface{}
	opsBufferSize    int
	bulkWriteChannel chan *SyncDoc
	bulkQuitChannel  chan chan bool
	bulk             bool

	restartable bool // this refers to being able to refresh the iterator, not to the restart based on session op
}

type SyncDoc struct {
	Doc        map[string]interface{}
	Collection string
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
		opsBuffer:        make(map[string][]interface{}),
		bulkWriteChannel: make(chan *SyncDoc),
		bulkQuitChannel:  make(chan chan bool),
		bulk:             conf.Bulk,
	}
	// opsBuffer:        make([]*SyncDoc, 0, MONGO_BUFFER_LEN),

	m.database, m.collectionMatch, err = extra.compileNamespace()
	if err != nil {
		return m, err
	}

	dialInfo, err := mgo.ParseURL(m.uri)
	if err != nil {
		return m, fmt.Errorf("unable to parse uri (%s), %s\n", m.uri, err.Error())
	}

	if conf.Ssl != nil {
		tlsConfig := &tls.Config{}
		if len(conf.Ssl.CaCerts) > 0 {
			roots := x509.NewCertPool()
			for _, caCert := range conf.Ssl.CaCerts {
				ok := roots.AppendCertsFromPEM([]byte(caCert))
				if !ok {
					return m, fmt.Errorf("failed to parse root certificate")
				}
			}
			tlsConfig.RootCAs = roots
		} else {
			tlsConfig.InsecureSkipVerify = true
		}
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			return conn, err
		}
	}

	if conf.Timeout == "" {
		dialInfo.Timeout = time.Duration(10) * time.Second
	} else {
		timeout, err := time.ParseDuration(conf.Timeout)
		if err != nil {
			return m, fmt.Errorf("unable to parse timeout (%s), %s\n", conf.Timeout, err.Error())
		}
		dialInfo.Timeout = timeout
	}

	m.mongoSession, err = mgo.DialWithInfo(dialInfo)
	if err != nil {
		return m, err
	}

	// set some options on the session
	m.mongoSession.EnsureSafe(&mgo.Safe{W: conf.Wc, FSync: conf.FSync})
	m.mongoSession.SetBatch(1000)
	m.mongoSession.SetPrefetch(0.5)

	if m.tail {
		if iter := m.mongoSession.DB("local").C("oplog.rs").Find(bson.M{}).Limit(1).Iter(); iter.Err() != nil {
			return m, iter.Err()
		}
	}

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
	return m.pipe.Listen(m.writeMessage, m.collectionMatch)
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
	_, msgColl, err := msg.SplitNamespace()
	if err != nil {
		m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (msg namespace improperly formatted, must be database.collection, got %s)", msg.Namespace), msg.Data)
		return msg, nil
	}

	collection := m.mongoSession.DB(m.database).C(msgColl)

	if !msg.IsMap() {
		m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (document must be a bson document, got %T instead)", msg.Data), msg.Data)
		return msg, nil
	}

	doc := &SyncDoc{
		Doc:        msg.Map(),
		Collection: msgColl,
	}

	if m.bulk {
		m.bulkWriteChannel <- doc
	} else if msg.Op == message.Delete {
		err := collection.Remove(doc.Doc)
		if err != nil {
			m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error removing (%s)", err.Error()), msg.Data)
		}
	} else {
		err := collection.Insert(doc.Doc)
		if mgo.IsDup(err) {
			err = collection.Update(bson.M{"_id": doc.Doc["_id"]}, doc.Doc)
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
			sz, err := docSize(doc.Doc)
			if err != nil {
				m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (%s)", err.Error()), doc)
				break
			}

			if ((sz + m.opsBufferSize) > MONGO_BUFFER_SIZE) || (m.opsBufferCount == MONGO_BUFFER_LEN) {
				m.writeBuffer() // send it off to be inserted
			}

			m.buffLock.Lock()
			m.opsBufferCount += 1
			m.opsBuffer[doc.Collection] = append(m.opsBuffer[doc.Collection], doc.Doc)
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
	for coll, docs := range m.opsBuffer {

		collection := m.mongoSession.DB(m.database).C(coll)
		if len(docs) == 0 {
			continue
		}

		err := collection.Insert(docs...)

		if err != nil {
			if mgo.IsDup(err) {
				err = nil
				for _, op := range docs {
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
				m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("mongodb error (%s)", err.Error()), docs[0])
			}
		}

	}

	m.opsBufferCount = 0
	m.opsBuffer = make(map[string][]interface{})
	m.opsBufferSize = 0
}

// catdata pulls down the original collections
func (m *Mongodb) catData() (err error) {
	collections, _ := m.mongoSession.DB(m.database).CollectionNames()
	for _, collection := range collections {
		if strings.HasPrefix(collection, "system.") {
			continue
		} else if match := m.collectionMatch.MatchString(collection); !match {
			continue
		}

		var (
			query  = bson.M{}
			result bson.M // hold the document
		)

		iter := m.mongoSession.DB(m.database).C(collection).Find(query).Sort("_id").Iter()

		for {
			for iter.Next(&result) {
				if stop := m.pipe.Stopped; stop {
					return
				}

				// set up the message
				msg := message.NewMsg(message.Insert, result, m.computeNamespace(collection))

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
				iter = m.mongoSession.DB(m.database).C(collection).Find(query).Sort("_id").Iter()
				continue
			}
			break
		}
	}
	return
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
		}

		iter = collection.Find(query).LogReplay().Sort("$natural").Tail(m.oplogTimeout)
	)

	for {
		for iter.Next(&result) {
			if stop := m.pipe.Stopped; stop {
				return
			}
			if result.validOp() {
				_, coll, _ := m.splitNamespace(result.Ns)

				if strings.HasPrefix(coll, "system.") {
					continue
				} else if match := m.collectionMatch.MatchString(coll); !match {
					continue
				}

				var doc bson.M
				switch result.Op {
				case "i":
					doc = result.O
				case "d":
					doc = result.O
				case "u":
					doc, err = m.getOriginalDoc(result.O2, coll)
					if err != nil { // errors aren't fatal here, but we need to send it down the pipe
						m.pipe.Err <- NewError(ERROR, m.path, fmt.Sprintf("Mongodb error (%s)", err.Error()), nil)
						continue
					}
				default:
					m.pipe.Err <- NewError(ERROR, m.path, "Mongodb error (unknown op type)", nil)
					continue
				}

				msg := message.NewMsg(message.OpTypeFromString(result.Op), doc, m.computeNamespace(coll))
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
		}
		iter = collection.Find(query).LogReplay().Tail(m.oplogTimeout)
	}
}

// getOriginalDoc retrieves the original document from the database.  transport has no knowledge of update operations, all updates
// work as wholesale document replaces
func (m *Mongodb) getOriginalDoc(doc bson.M, collection string) (result bson.M, err error) {
	id, exists := doc["_id"]
	if !exists {
		return result, fmt.Errorf("can't get _id from document")
	}

	err = m.mongoSession.DB(m.database).C(collection).FindId(id).One(&result)
	if err != nil {
		err = fmt.Errorf("%s.%s %v %v", m.database, collection, id, err)
	}
	return
}

func (m *Mongodb) computeNamespace(collection string) string {
	return strings.Join([]string{m.database, collection}, ".")
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
	URI       string     `json:"uri" doc:"the uri to connect to, in the form mongodb://user:password@host.com:27017/auth_database"`
	Namespace string     `json:"namespace" doc:"mongo namespace to read/write"`
	Ssl       *SslConfig `json:"ssl,omitempty" doc:"ssl options for connection"`
	Timeout   string     `json:timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	Debug     bool       `json:"debug" doc:"display debug information"`
	Tail      bool       `json:"tail" doc:"if tail is true, then the mongodb source will tail the oplog after copying the namespace"`
	Wc        int        `json:"wc" doc:"The write concern to use for writes, Int, indicating the minimum number of servers to write to before returning success/failure"`
	FSync     bool       `json:"fsync" doc:"When writing, should we flush to disk before returning success"`
	Bulk      bool       `json:"bulk" doc:"use a buffer to bulk insert documents"`
}

type SslConfig struct {
	CaCerts []string `json:"cacerts,omitempty" doc:"array of root CAs to use in order to verify the server certificates"`
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
