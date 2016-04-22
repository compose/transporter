package mongodb

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type MongoMessage struct {
	TS        int64
	BSONData  data.BSONData
	NS        string
	Operation ops.Op
}

func (r *MongoMessage) Timestamp() int64 {
	return r.TS
}

func (r *MongoMessage) Data() interface{} {
	return r.BSONData
}

func (r *MongoMessage) Namespace() string {
	return r.NS
}

func (r *MongoMessage) OP() ops.Op {
	return r.Operation
}

func (r *MongoMessage) ID() string {
	switch r := r.BSONData["_id"].(type) {
	case string:
		return r
	case bson.ObjectId:
		return string(r)
	default:
		return fmt.Sprintf("%v", r)
	}
}
