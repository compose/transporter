package rethinkdb

import (
	"fmt"

	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type RethinkMessage struct {
	TS        int64
	MapData   data.MapData
	NS        string
	Operation ops.Op
}

func (r *RethinkMessage) Timestamp() int64 {
	return r.TS
}

func (r *RethinkMessage) Data() interface{} {
	return r.MapData
}

func (r *RethinkMessage) Namespace() string {
	return r.NS
}

func (r *RethinkMessage) OP() ops.Op {
	return r.Operation
}

func (r *RethinkMessage) ID() string {
	switch r := r.MapData["id"].(type) {
	case string:
		return r
	default:
		return fmt.Sprintf("%v", r)
	}
}
