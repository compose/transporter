package mongodb

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

// From builds a message.Msg specific to a MongoDB document
func From(op ops.Op, namespace string, d data.Data) message.Msg {
	return &Message{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
		BSONData:  d,
	}
}

// Message implements the message.Msg interface for use with MongoDB data.
type Message struct {
	TS        int64
	BSONData  data.Data
	NS        string
	Operation ops.Op
}

// Timestamp returns the time the object was created in transporter (i.e. it has no correlation
// with any time in the database).
func (r *Message) Timestamp() int64 {
	return r.TS
}

// Data returns the internal representation of a MongoDB document as the data.Data type
func (r *Message) Data() data.Data {
	return r.BSONData
}

// Namespace returns the MongoDB database and collection joined by a '.'.
func (r *Message) Namespace() string {
	return r.NS
}

// OP returns the type of operation the message is associated with (i.e. insert/update/delete).
func (r *Message) OP() ops.Op {
	return r.Operation
}

// ID will attempt to convert the _id field into a string representation
func (r *Message) ID() string {
	switch r := r.BSONData.Get("_id").(type) {
	case string:
		return r
	case bson.ObjectId:
		return r.Hex()
	default:
		return fmt.Sprintf("%v", r)
	}
}
