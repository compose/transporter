package mongodb

import (
	"math"
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
	lock    sync.Mutex
}

type bulkOperation struct {
	s          *mgo.Session
	bulk       *mgo.Bulk
	opCounter  float64
	avgOpCount int
	avgTotal   int
	avgOpSize  float64
	bsonOpSize int
}

func newBulker(done chan struct{}, wg *sync.WaitGroup) *Bulk {
	b := &Bulk{
		bulkMap: make(map[string]*bulkOperation),
	}
	go b.run(done, wg)
	return b
}

func (b *Bulk) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		db, coll, _ := message.SplitNamespace(msg)
		b.lock.Lock()
		bOp, ok := b.bulkMap[coll]
		if !ok {
			s := s.(*Session).mgoSession.Copy()
			bOp = &bulkOperation{
				s:    s,
				bulk: s.DB(db).C(coll).Bulk(),
			}
			b.bulkMap[coll] = bOp
		}
		switch msg.OP() {
		case ops.Delete:
			bOp.bulk.Remove(bson.M{"_id": msg.Data().Get("_id")})
		case ops.Insert:
			bOp.bulk.Insert(msg.Data())
		case ops.Update:
			bOp.bulk.Update(bson.M{"_id": msg.Data().Get("_id")}, msg.Data())
		}
		if math.Mod(bOp.opCounter, 20.0) == 0 {
			log.With("opCounter", bOp.opCounter).Debugln("calculating avg obj size")
			bOp.calculateAvgObjSize(msg.Data())
		}
		bOp.opCounter++
		bOp.bsonOpSize = int(bOp.avgOpSize) * int(bOp.opCounter)
		b.lock.Unlock()
		if int(bOp.opCounter) >= maxObjSize || bOp.bsonOpSize >= maxBSONObjSize {
			return b.flush(coll, bOp)
		}
		return nil
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
	wg.Add(1)
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
		b.flush(c, bOp)
	}
	return nil
}

func (b *Bulk) flush(c string, bOp *bulkOperation) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	log.With("collection", c).With("opCounter", bOp.opCounter).With("bsonOpSize", bOp.bsonOpSize).Debugln("flushing bulk messages")
	result, err := bOp.bulk.Run()
	if err != nil {
		log.With("collection", c).Errorf("flush error, %s\n", err)
		return err
	}
	bOp.s.Close()
	log.With("collection", c).
		With("modified", result.Modified).
		With("match", result.Matched).
		Debugln("flush complete")
	delete(b.bulkMap, c)
	return nil
}
