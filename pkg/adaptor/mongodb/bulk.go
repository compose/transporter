package mongodb

import (
	"sync"
	"time"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	_ client.Writer = &Bulk{}
)

// Bulk implements client.Writer for use with MongoDB and takes advantage of the Bulk API for
// performance improvements.
type Bulk struct {
	bulkMap   map[string]*bulkOperation
	lock      sync.Mutex
	opCounter int
}

type bulkOperation struct {
	s    *mgo.Session
	bulk *mgo.Bulk
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
		b.opCounter++
		b.lock.Unlock()
		if b.opCounter == 1000 {
			b.flush()
		}
		return nil
	}
}

func (b *Bulk) run(done chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	for {
		select {
		case <-time.After(2 * time.Second):
			b.flush()
		case <-done:
			log.Debugln("received done channel")
			b.flush()
			wg.Done()
			return
		}
	}
}

func (b *Bulk) flush() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	log.Debugln("flushing bulk messages")
	for coll, bulkOp := range b.bulkMap {
		log.With("collection", coll).Debugln("flushing messages")
		result, err := bulkOp.bulk.Run()
		if err != nil {
			log.With("collection", coll).Errorf("flush error, %s\n", err)
			return err
		}
		bulkOp.s.Close()
		log.With("collection", coll).
			With("modified", result.Modified).
			With("match", result.Matched).
			Debugln("flush complete")
	}
	b.bulkMap = make(map[string]*bulkOperation)
	b.opCounter = 0
	return nil
}
