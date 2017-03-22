package mongodb

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	_ client.Reader = &Reader{}

	// DefaultCollectionFilter is an empty map of empty maps
	DefaultCollectionFilter = map[string]CollectionFilter{}
)

// CollectionFilter is just a typed map of strings of map[string]interface{}
type CollectionFilter map[string]interface{}

// Reader implements the behavior defined by client.Reader for interfacing with MongoDB.
type Reader struct {
	tail              bool
	collectionFilters map[string]CollectionFilter
	oplogTimeout      time.Duration
}

func newReader(tail bool, filters map[string]CollectionFilter) client.Reader {
	return &Reader{tail, filters, 5 * time.Second}
}

type resultDoc struct {
	doc bson.M
	c   string
}

type iterationComplete struct {
	oplogTime bson.MongoTimestamp
	c         string
}

func (r *Reader) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		session := s.(*Session).mgoSession.Copy()
		go func() {
			defer func() {
				session.Close()
				close(out)
			}()
			log.With("db", session.DB("").Name).Infoln("starting Read func")
			collections, err := r.listCollections(session.Copy(), filterFn)
			if err != nil {
				log.With("db", session.DB("").Name).Errorf("unable to list collections, %s", err)
				return
			}
			var wg sync.WaitGroup
			for _, c := range collections {
				oplogTime := timeAsMongoTimestamp(time.Now())
				if err := r.iterateCollection(session.Copy(), c, out, done); err != nil {
					log.With("db", session.DB("").Name).Errorln(err)
					return
				}
				log.With("db", session.DB("").Name).With("collection", c).Infoln("iterating complete")
				if r.tail {
					wg.Add(1)
					log.With("collection", c).Infof("oplog start timestamp: %d", oplogTime)
					go func(wg *sync.WaitGroup, c string, o bson.MongoTimestamp) {
						defer wg.Done()
						errc := r.tailCollection(c, session.Copy(), o, out, done)
						for err := range errc {
							log.With("db", session.DB("").Name).With("collection", c).Errorln(err)
							return
						}
					}(&wg, c, oplogTime)
				}
			}
			log.With("db", session.DB("").Name).Infoln("Read completed")
			// this will block if we're tailing
			wg.Wait()
			return
		}()

		return out, nil
	}
}

func (r *Reader) listCollections(mgoSession *mgo.Session, filterFn func(name string) bool) ([]string, error) {
	defer mgoSession.Close()
	var colls []string
	db := mgoSession.DB("")
	collections, err := db.CollectionNames()
	if err != nil {
		return colls, err
	}
	log.With("db", db.Name).With("num_collections", len(collections)).Infoln("collection count")
	for _, c := range collections {
		if filterFn(c) && !strings.HasPrefix(c, "system.") {
			log.With("db", db.Name).With("collection", c).Infoln("adding for iteration...")
			colls = append(colls, c)
		} else {
			log.With("db", db.Name).With("collection", c).Infoln("skipping iteration...")
		}
	}
	log.With("db", db.Name).Infoln("done iterating collections")
	return colls, nil
}

func (r *Reader) iterateCollection(s *mgo.Session, c string, out chan<- client.MessageSet, done chan struct{}) error {
	iter := r.iterate(s, c)
	for {
		select {
		case msg, ok := <-iter:
			if !ok {
				return nil
			}
			out <- client.MessageSet{
				Msg: msg,
			}
		case <-done:
			return errors.New("iteration cancelled")
		}
	}
}

func (r *Reader) iterate(s *mgo.Session, c string) <-chan message.Msg {
	msgChan := make(chan message.Msg)
	go func() {
		defer func() {
			s.Close()
			close(msgChan)
		}()
		db := s.DB("").Name
		canReissueQuery := r.requeryable(c, s)
		var lastID interface{}
		for {
			log.With("collection", c).Infoln("iterating...")
			session := s.Copy()
			iter := r.catQuery(c, lastID, session).Iter()
			var result bson.M
			for iter.Next(&result) {
				if id, ok := result["_id"]; ok {
					lastID = id
				}
				msgChan <- message.From(ops.Insert, c, data.Data(result))
				result = bson.M{}
			}
			if err := iter.Err(); err != nil {
				log.With("database", db).With("collection", c).Errorf("error reading, %s", err)
				session.Close()
				if canReissueQuery {
					log.With("database", db).With("collection", c).Errorln("attempting to reissue query")
					time.Sleep(5 * time.Second)
					continue
				}
				return
			}
			iter.Close()
			session.Close()
			return
		}
	}()
	return msgChan
}

func (r *Reader) catQuery(c string, lastID interface{}, mgoSession *mgo.Session) *mgo.Query {
	query := bson.M{}
	if f, ok := r.collectionFilters[c]; ok {
		query = bson.M(f)
	}
	if lastID != nil {
		query["_id"] = bson.M{"$gt": lastID}
	}
	return mgoSession.DB("").C(c).Find(query).Sort("_id")
}

func (r *Reader) requeryable(c string, mgoSession *mgo.Session) bool {
	db := mgoSession.DB("")
	indexes, err := db.C(c).Indexes()
	if err != nil {
		log.With("database", db.Name).With("collection", c).Errorf("unable to list indexes, %s", err)
		return false
	}
	for _, index := range indexes {
		if index.Key[0] == "_id" {
			var result bson.M
			err := db.C(c).Find(nil).Select(bson.M{"_id": 1}).One(&result)
			if err != nil {
				log.With("database", db.Name).With("collection", c).Errorf("unable to sample document, %s", err)
				break
			}
			if id, ok := result["_id"]; ok && sortable(id) {
				return true
			}
			break
		}
	}
	log.With("database", db.Name).With("collection", c).Infoln("invalid _id, any issues copying will be aborted")
	return false
}

func sortable(id interface{}) bool {
	switch id.(type) {
	case bson.ObjectId, string, float64, int64, time.Time:
		return true
	}
	return false
}

func (r *Reader) tailCollection(c string, mgoSession *mgo.Session, oplogTime bson.MongoTimestamp, out chan<- client.MessageSet, done chan struct{}) chan error {
	errc := make(chan error)
	go func() {
		defer func() {
			mgoSession.Close()
			close(errc)
		}()

		var (
			collection = mgoSession.DB("local").C("oplog.rs")
			result     oplogDoc // hold the document
			db         = mgoSession.DB("").Name
			query      = bson.M{"ns": fmt.Sprintf("%s.%s", db, c), "ts": bson.M{"$gte": oplogTime}}
			iter       = collection.Find(query).LogReplay().Sort("$natural").Tail(r.oplogTimeout)
		)
		defer iter.Close()

		for {
			log.With("db", db).Infof("tailing oplog with query %+v", query)
			select {
			case <-done:
				log.With("db", db).Infoln("tailing stopping...")
				return
			default:
				for iter.Next(&result) {
					if result.validOp() {
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
							doc, err = r.getOriginalDoc(result.O2, c, mgoSession)
							if err != nil {
								// errors aren't fatal here, but we need to send it down the pipe
								log.With("ns", result.Ns).Errorf("unable to getOriginalDoc, %s", err)
								// m.pipe.Err <- adaptor.NewError(adaptor.ERROR, m.path, fmt.Sprintf("tail MongoDB error (%s)", err.Error()), nil)
								continue
							}
						}

						msg := message.From(op, c, data.Data(doc)).(*message.Base)
						msg.TS = int64(result.Ts) >> 32

						out <- client.MessageSet{
							Msg:       msg,
							Timestamp: msg.TS,
							Mode:      commitlog.Sync,
						}
						oplogTime = result.Ts
					}
					result = oplogDoc{}
				}
			}

			if iter.Timeout() {
				continue
			}
			if iter.Err() != nil {
				log.With("path", db).Errorf("error tailing oplog, %s", iter.Err())
				// return adaptor.NewError(adaptor.CRITICAL, m.path, fmt.Sprintf("MongoDB error (error reading collection %s)", iter.Err()), nil)
			}

			query = bson.M{"ts": bson.M{"$gte": oplogTime}}
			iter = collection.Find(query).LogReplay().Tail(r.oplogTimeout)
			time.Sleep(100 * time.Millisecond)
		}

	}()
	return errc
}

// getOriginalDoc retrieves the original document from the database.
// transporter has no knowledge of update operations, all updates work as wholesale document replaces
func (r *Reader) getOriginalDoc(doc bson.M, c string, s *mgo.Session) (result bson.M, err error) {
	id, exists := doc["_id"]
	if !exists {
		return result, fmt.Errorf("can't get _id from document")
	}

	query := bson.M{}
	if f, ok := r.collectionFilters[c]; ok {
		query = bson.M(f)
	}
	query["_id"] = id

	err = s.DB("").C(c).Find(query).One(&result)
	if err != nil {
		err = fmt.Errorf("%s.%s %v %v", s.DB("").Name, c, id, err)
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

func timeAsMongoTimestamp(t time.Time) bson.MongoTimestamp {
	return bson.MongoTimestamp(t.Unix() << 32)
}
