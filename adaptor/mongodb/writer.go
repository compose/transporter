package mongodb

import (
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
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

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		writeFunc, ok := w.writeMap[msg.OP()]
		if !ok {
			log.Infof("no function registered for operation, %s\n", msg.OP())
			if msg.Confirms() != nil {
				close(msg.Confirms())
			}
			return msg, nil
		}
		if err := writeFunc(msg, msgCollection(msg, s)); err != nil {
			return nil, err
		}
		if msg.Confirms() != nil {
			close(msg.Confirms())
		}
		return msg, nil
	}
}

func msgCollection(msg message.Msg, s client.Session) *mgo.Collection {
	return s.(*Session).mgoSession.DB("").C(msg.Namespace())
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
