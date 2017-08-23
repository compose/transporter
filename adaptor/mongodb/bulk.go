package mongodb

import (
	"sync"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
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
	confirmChan chan struct{}
}

type bulkOperation struct {
	s          *mgo.Session
	bulk       *mgo.Bulk
	opCounter  int
	avgOpCount int
	avgTotal   int
	avgOpSize  float64
	bsonOpSize int
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

func (b *Bulk) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		coll := msg.Namespace()
		b.Lock()
		b.confirmChan = msg.Confirms()
		bOp, ok := b.bulkMap[coll]
		if !ok {
			s := s.(*Session).mgoSession.Copy()
			bOp = &bulkOperation{
				s:    s,
				bulk: s.DB("").C(coll).Bulk(),
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
		bOp.opCounter++
		if bOp.opCounter%20 == 0 {
			log.With("opCounter", bOp.opCounter).Debugln("calculating avg obj size")
			bOp.calculateAvgObjSize(msg.Data())
		}
		bOp.bsonOpSize = int(bOp.avgOpSize) * bOp.opCounter
		var err error
		if bOp.opCounter >= maxObjSize || bOp.bsonOpSize >= maxBSONObjSize {
			err = b.flush(coll, bOp)
			if err == nil && b.confirmChan != nil {
				b.confirmChan <- struct{}{}
			}
		}
		b.Unlock()
		return msg, err
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
	defer wg.Done()
	for {
		select {
		case <-time.After(2 * time.Second):
			if err := b.flushAll(); err != nil {
				log.Errorf("flush error, %s", err)
				return
			}
		case <-done:
			log.Debugln("received done channel")
			if err := b.flushAll(); err != nil {
				log.Errorf("flush error, %s", err)
			}
			return
		}
	}
}

func (b *Bulk) flushAll() error {
	b.Lock()
	for c, bOp := range b.bulkMap {
		if err := b.flush(c, bOp); err != nil {
			return err
		}
	}
	if b.confirmChan != nil {
		b.confirmChan <- struct{}{}
	}
	b.Unlock()
	return nil
}

func (b *Bulk) flush(c string, bOp *bulkOperation) error {
	log.With("collection", c).With("opCounter", bOp.opCounter).With("bsonOpSize", bOp.bsonOpSize).Debugln("flushing bulk messages")
	_, err := bOp.bulk.Run()
	if err != nil && !mgo.IsDup(err) {
		log.With("collection", c).Errorf("flush error, %s\n", err)
		return err
	} else if mgo.IsDup(err) {
		bOp.bulk.Unordered()
		if _, err := bOp.bulk.Run(); err != nil && !mgo.IsDup(err) {
			log.With("collection", c).Errorf("flush error with unordered, %s\n", err)
			return err
		}
	}
	bOp.s.Close()
	log.With("collection", c).Debugln("flush complete")
	delete(b.bulkMap, c)
	return nil
}
