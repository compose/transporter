package mongodb

import (
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var _ client.Writer = &Writer{}

// Writer implements client.Writer for use with MongoDB
type Writer struct {
	writeMap map[ops.Op]func(message.Msg, *mgo.Collection) error
}

func newWriter() *Writer {
	w := &Writer{}
	w.writeMap = map[ops.Op]func(message.Msg, *mgo.Collection) error{
		ops.Insert: insertMsg,
		ops.Update: updateMsg,
		ops.Delete: deleteMsg,
	}
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		w, ok := w.writeMap[msg.OP()]
		if !ok {
			log.Infof("no function registered for operation, %s\n", msg.OP())
			return nil
		}
		return w(msg, msgCollection(msg, s))
	}
}

func msgCollection(msg message.Msg, s client.Session) *mgo.Collection {
	db, coll, _ := message.SplitNamespace(msg)
	return s.(*Session).mgoSession.DB(db).C(coll)
}

func insertMsg(msg message.Msg, c *mgo.Collection) error {
	err := c.Insert(msg.Data())
	if err != nil && mgo.IsDup(err) {
		return updateMsg(msg, c)
	}
	return err
}

func updateMsg(msg message.Msg, c *mgo.Collection) error {
	return c.Update(bson.M{"_id": msg.Data().Get("_id")}, msg.Data())
}

func deleteMsg(msg message.Msg, c *mgo.Collection) error {
	return c.RemoveId(msg.Data().Get("_id"))
}
