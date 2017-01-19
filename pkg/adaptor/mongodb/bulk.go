package mongodb

import (
	"sync"
	"time"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	maxObjSize     int = 1000
	maxBSONObjSize int = 1e6
)

var (
	_ client.Writer = &Bulk{}
)

// Bulk implements client.Writer for use with MongoDB and takes advantage of the Bulk API for
// performance improvements.
type Bulk struct {
	bulkMap map[string]*bulkOperation
	*sync.RWMutex
}

type bulkOperation struct {
	s          *mgo.Session
	bulk       *mgo.Bulk
	opCounter  int
	avgOpCount int
	avgTotal   int
	avgOpSize  float64
	bsonOpSize int
	*sync.Mutex
}

func newBulker(done chan struct{}, wg *sync.WaitGroup) *Bulk {
	b := &Bulk{
		bulkMap: make(map[string]*bulkOperation),
		RWMutex: &sync.RWMutex{},
	}
	wg.Add(1)
	go b.run(done, wg)
	return b
}

func (b *Bulk) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		db, coll, _ := message.SplitNamespace(msg)
		b.RLock()
		bOp, ok := b.bulkMap[coll]
		b.RUnlock()
		if !ok {
			s := s.(*Session).mgoSession.Copy()
			bOp = &bulkOperation{
				s:     s,
				bulk:  s.DB(db).C(coll).Bulk(),
				Mutex: &sync.Mutex{},
			}
			b.Lock()
			b.bulkMap[coll] = bOp
			b.Unlock()
		}
		bOp.Lock()
		switch msg.OP() {
		case ops.Delete:
			bOp.bulk.Remove(bson.M{"_id": msg.Data().Get("_id")})
		case ops.Insert:
			bOp.bulk.Insert(msg.Data())
		case ops.Update:
			bOp.bulk.Update(bson.M{"_id": msg.Data().Get("_id")}, msg.Data())
		}
		if bOp.opCounter%20 == 0 {
			log.With("opCounter", bOp.opCounter).Debugln("calculating avg obj size")
			bOp.calculateAvgObjSize(msg.Data())
		}
		bOp.opCounter++
		bOp.bsonOpSize = int(bOp.avgOpSize) * bOp.opCounter
		bOp.Unlock()
		var err error
		if bOp.opCounter >= maxObjSize || bOp.bsonOpSize >= maxBSONObjSize {
			err = b.flush(coll, bOp)
		}
		return err
	}
}

func (bOp *bulkOperation) calculateAvgObjSize(d data.Data) {
	bs, err := bson.Marshal(d)
	if err != nil {
		log.Infof("unable to marshal doc to BSON, not adding to average", err)
		return
	}
	bOp.avgOpCount++
	// add the 4 bytes for the MsgHeader
	// https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#standard-message-header
	bOp.avgTotal += (len(bs) + 4)
	bOp.avgOpSize = float64(bOp.avgTotal / bOp.avgOpCount)
	log.With("avgOpCount", bOp.avgOpCount).With("avgTotal", bOp.avgTotal).With("avgObSize", bOp.avgOpSize).Debugln("bulk stats")
}

func (b *Bulk) run(done chan struct{}, wg *sync.WaitGroup) {
	for {
		select {
		case <-time.After(2 * time.Second):
			b.flushAll()
		case <-done:
			log.Debugln("received done channel")
			b.flushAll()
			wg.Done()
			return
		}
	}
}

func (b *Bulk) flushAll() error {
	for c, bOp := range b.bulkMap {
		bOp.Lock()
		b.flush(c, bOp)
		bOp.Unlock()
	}
	return nil
}

func (b *Bulk) flush(c string, bOp *bulkOperation) error {
	log.With("collection", c).With("opCounter", bOp.opCounter).With("bsonOpSize", bOp.bsonOpSize).Infoln("flushing bulk messages")
	result, err := bOp.bulk.Run()
	if err != nil {
		log.With("collection", c).Errorf("flush error, %s\n", err)
		return err
	}
	bOp.s.Close()
	log.With("collection", c).
		With("modified", result.Modified).
		With("match", result.Matched).
		Infoln("flush complete")
	b.Lock()
	delete(b.bulkMap, c)
	b.Unlock()
	return nil
}
