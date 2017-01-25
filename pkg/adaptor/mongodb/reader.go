package mongodb

import (
	"fmt"
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
	_ client.Reader = &Reader{}
)

// Reader implements the behavior defined by client.Reader for interfacing with MongoDB.
type Reader struct {
	db string
}

func newReader(db string) client.Reader {
	return &Reader{db}
}

type resultDoc struct {
	doc bson.M
	c   string
}

func (r *Reader) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan message.Msg, error) {
		out := make(chan message.Msg)
		session := s.(*Session)
		go func() {
			defer close(out)
			log.With("db", r.db).Infoln("starting Read func")
			cSession := session.mgoSession.Copy()
			defer cSession.Close()
			collections, err := r.listCollections(cSession, filterFn)
			if err != nil {
				log.With("db", r.db).Errorf("unable to list collections, %s", err)
				return
			}
			iSession := session.mgoSession.Copy()
			defer iSession.Close()
			results := r.iterateCollection(iSession, collections, done)
			for {
				select {
				case <-done:
					return
				case result, ok := <-results:
					if !ok {
						log.With("db", r.db).Infoln("Read completed")
						return
					}
					msg := message.From(ops.Insert, fmt.Sprintf("%s.%s", r.db, result.c), data.Data(result.doc))
					out <- msg
				}
			}
		}()

		return out, nil
	}
}

func (r *Reader) listCollections(mgoSession *mgo.Session, filterFn func(name string) bool) (<-chan string, error) {
	out := make(chan string)
	collections, err := mgoSession.DB(r.db).CollectionNames()
	if err != nil {
		return out, err
	}
	log.With("db", r.db).With("num_collections", len(collections)).Infoln("collection count")
	go func() {
		defer close(out)
		for _, c := range collections {
			if filterFn(c) {
				log.With("db", r.db).With("collection", c).Infoln("sending for iteration...")
				out <- c
			} else {
				log.With("db", r.db).With("collection", c).Infoln("skipping iteration...")
			}
		}
		log.With("db", r.db).Infoln("done iterating collections")
	}()
	return out, nil
}

func (r *Reader) iterateCollection(mgoSession *mgo.Session, in <-chan string, done chan struct{}) <-chan resultDoc {
	out := make(chan resultDoc)
	go func() {
		defer close(out)
		for {
			select {
			case c, ok := <-in:
				if !ok {
					return
				}
				log.With("collection", c).Infoln("iterating...")
				canReissueQuery := r.requeryable(c, mgoSession)
				var lastID interface{}
				for {
					s := mgoSession.Copy()
					iter := r.catQuery(c, lastID, s).Iter()
					var result bson.M
					for iter.Next(&result) {
						if id, ok := result["_id"]; ok {
							lastID = id
						}
						out <- resultDoc{result, c}
						result = bson.M{}
					}
					if err := iter.Err(); err != nil {
						log.With("database", r.db).With("collection", c).Errorf("error reading, %s\n", err)
						s.Close()
						if canReissueQuery {
							log.With("database", r.db).With("collection", c).Errorln("attempting to reissue query")
							time.Sleep(5 * time.Second)
							continue
						}
						break
					}
					iter.Close()
					s.Close()
					break
				}
				log.With("collection", c).Infoln("iterating complete")
			case <-done:
				log.With("db", r.db).Infoln("iterating no more")
				return
			}
		}
	}()
	return out
}

func (r *Reader) catQuery(c string, lastID interface{}, mgoSession *mgo.Session) *mgo.Query {
	query := bson.M{}
	if lastID != nil {
		query = bson.M{"_id": bson.M{"$gte": lastID}}
	}
	return mgoSession.DB(r.db).C(c).Find(query).Sort("_id")
}

func (r *Reader) requeryable(c string, mgoSession *mgo.Session) bool {
	indexes, err := mgoSession.DB(r.db).C(c).Indexes()
	if err != nil {
		log.With("database", r.db).With("collection", c).Errorf("unable to list indexes, %s\n", err)
		return false
	}
	for _, index := range indexes {
		if index.Key[0] == "_id" {
			var result bson.M
			err := mgoSession.DB(r.db).C(c).Find(nil).Select(bson.M{"_id": 1}).One(&result)
			if err != nil {
				fmt.Printf("[ERROR] unable to sample document, %s", err)
				break
			}
			if id, ok := result["_id"]; ok && sortable(id) {
				return true
			}
			break
		}
	}
	log.With("database", r.db).With("collection", c).Infoln("invalid _id, any issues copying will be aborted")
	return false
}

func sortable(id interface{}) bool {
	switch id.(type) {
	case bson.ObjectId, string, float64, int64, time.Time:
		return true
	}
	return false
}
