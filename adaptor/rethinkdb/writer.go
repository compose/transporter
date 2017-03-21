package rethinkdb

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"

	r "gopkg.in/gorethink/gorethink.v3"
)

const (
	maxObjSize int = 1000
)

var (
	_ client.Writer = &Writer{}
)

// Writer implements client.Writer for use with RethinkDB
type Writer struct {
	bulkMap map[string]*bulkOperation
	*sync.Mutex
	opCounter int
}

type bulkOperation struct {
	s    *r.Session
	docs []map[string]interface{}
}

func newWriter(done chan struct{}, wg *sync.WaitGroup) *Writer {
	w := &Writer{
		bulkMap: make(map[string]*bulkOperation),
		Mutex:   &sync.Mutex{},
	}
	wg.Add(1)
	go w.run(done, wg)
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		table := msg.Namespace()
		rSession := s.(*Session).session
		switch msg.OP() {
		case ops.Delete:
			w.flushAll()
			return msg, do(r.DB(rSession.Database()).Table(table).Get(prepareDocument(msg)["id"]).Delete(), rSession)
		case ops.Insert:
			w.Lock()
			bOp, ok := w.bulkMap[table]
			if !ok {
				bOp = &bulkOperation{
					s:    rSession,
					docs: make([]map[string]interface{}, 0),
				}
				w.bulkMap[table] = bOp
			}
			bOp.docs = append(bOp.docs, prepareDocument(msg))
			w.Unlock()
			w.opCounter++
			if w.opCounter >= maxObjSize {
				w.flushAll()
			}
		case ops.Update:
			w.flushAll()
			return msg, do(r.DB(rSession.Database()).Table(table).Insert(prepareDocument(msg), r.InsertOpts{Conflict: "replace"}), rSession)
		}
		return msg, nil
	}
}

// prepareDocument checks for an `_id` field and moves it to `id`.
func prepareDocument(msg message.Msg) map[string]interface{} {
	if _, ok := msg.Data()["id"]; ok {
		return msg.Data()
	}

	if _, ok := msg.Data()["_id"]; ok {
		msg.Data().Set("id", msg.ID())
		msg.Data().Delete("_id")
	}
	return msg.Data()
}

func (w *Writer) run(done chan struct{}, wg *sync.WaitGroup) {
	for {
		select {
		case <-time.After(2 * time.Second):
			if err := w.flushAll(); err != nil {
				log.Errorf("flush error, %s", err)
				return
			}
		case <-done:
			log.Debugln("received done channel")
			w.flushAll()
			wg.Done()
			return
		}
	}
}

func (w *Writer) flushAll() error {
	w.Lock()
	defer func() {
		w.opCounter = 0
		w.Unlock()
	}()
	for t, bOp := range w.bulkMap {
		log.With("db", bOp.s.Database()).With("table", t).With("op_counter", w.opCounter).With("doc_count", len(bOp.docs)).Infoln("flushing bulk messages")
		resp, err := r.DB(bOp.s.Database()).Table(t).Insert(bOp.docs, r.InsertOpts{Conflict: "replace"}).RunWrite(bOp.s)
		if err != nil {
			return err
		}
		if err := handleResponse(&resp); err != nil {
			return err
		}
	}
	w.bulkMap = make(map[string]*bulkOperation)
	return nil
}

func do(t r.Term, s *r.Session) error {
	resp, err := t.RunWrite(s)
	if err != nil {
		return err
	}
	return handleResponse(&resp)
}

// handleresponse takes the rethink response and turn it into something we can consume elsewhere
func handleResponse(resp *r.WriteResponse) error {
	if resp.Errors != 0 {
		if !strings.Contains(resp.FirstError, "Duplicate primary key") { // we don't care about this error
			return fmt.Errorf("%s\n%s", "problem inserting docs", resp.FirstError)
		}
	}
	return nil
}
