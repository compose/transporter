package mongodb

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"

	"git.compose.io/compose/transporter/pkg/message/data"
	"git.compose.io/compose/transporter/pkg/message/ops"
)

type Message struct {
	TS        int64
	BSONData  data.Data
	NS        string
	Operation ops.Op
}

func (r *Message) Timestamp() int64 {
	return r.TS
}

func (r *Message) Data() data.Data {
	return r.BSONData
}

func (r *Message) Namespace() string {
	return r.NS
}

func (r *Message) OP() ops.Op {
	return r.Operation
}

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
