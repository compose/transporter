package rethinkdb

import (
	"fmt"
	"strings"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"

	r "gopkg.in/gorethink/gorethink.v3"
)

var (
	_ client.Writer = &Writer{}
)

// Writer implements client.Writer for use with RethinkDB
type Writer struct {
	db       string
	writeMap map[ops.Op]func(message.Msg, r.Term, *r.Session) error
}

func newWriter(db string) *Writer {
	w := &Writer{db: db}
	w.writeMap = map[ops.Op]func(message.Msg, r.Term, *r.Session) error{
		ops.Insert: insertMsg,
		ops.Update: updateMsg,
		ops.Delete: deleteMsg,
	}
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		writeFunc, ok := w.writeMap[msg.OP()]
		if !ok {
			log.Infof("no function registered for operation, %s\n", msg.OP())
			return nil
		}
		tableTerm, session := msgTable(w.db, msg, s)
		return writeFunc(msg, tableTerm, session)
	}
}

func msgTable(db string, msg message.Msg, s client.Session) (r.Term, *r.Session) {
	return r.DB(db).Table(msg.Namespace()), s.(*Session).session
}

func insertMsg(msg message.Msg, t r.Term, s *r.Session) error {
	return do(t.Insert(prepareDocument(msg.Data()), r.InsertOpts{Conflict: "replace"}), s)
}

func updateMsg(msg message.Msg, t r.Term, s *r.Session) error {
	return do(t.Insert(prepareDocument(msg.Data()), r.InsertOpts{Conflict: "replace"}), s)
}

func deleteMsg(msg message.Msg, t r.Term, s *r.Session) error {
	return do(t.Get(prepareDocument(msg.Data())["id"]).Delete(), s)
}

// prepareDocument checks for an `_id` field and moves it to `id`.
func prepareDocument(doc map[string]interface{}) map[string]interface{} {
	if id, ok := doc["_id"]; ok {
		doc["id"] = id
		delete(doc, "_id")
	}
	return doc
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
