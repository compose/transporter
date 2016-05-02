package rethinkdb

import (
	"fmt"

	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type Message struct {
	TS        int64
	MapData   data.MapData
	NS        string
	Operation ops.Op
}

func (r *Message) Timestamp() int64 {
	return r.TS
}

func (r *Message) Data() interface{} {
	return r.MapData
}

func (r *Message) Namespace() string {
	return r.NS
}

func (r *Message) OP() ops.Op {
	return r.Operation
}

func (r *Message) ID() string {
	switch r := r.MapData["id"].(type) {
	case string:
		return r
	default:
		return fmt.Sprintf("%v", r)
	}
}
