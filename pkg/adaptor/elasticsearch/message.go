package elasticsearch

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

func From(op ops.Op, namespace string, d data.Data) message.Msg {
	return &Message{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
		MapData:   d,
	}
}

type Message struct {
	TS        int64
	MapData   data.Data
	NS        string
	Operation ops.Op
}

func (r *Message) Timestamp() int64 {
	return r.TS
}

func (r *Message) Data() data.Data {
	return r.MapData
}

func (r *Message) Namespace() string {
	return r.NS
}

func (r *Message) OP() ops.Op {
	return r.Operation
}

func (r *Message) ID() string {
	switch r := r.MapData["_id"].(type) {
	case string:
		return r
	case bson.ObjectId:
		return r.Hex()
	default:
		return fmt.Sprintf("%v", r)
	}
}
