package rethinkdb

import (
	"fmt"

	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type rethinkMessage struct {
	ts        int64
	d         data.MapData
	namespace string
	op        ops.Op
}

func (r *rethinkMessage) Timestamp() int64 {
	return r.ts
}

func (r *rethinkMessage) Data() interface{} {
	return r.d
}

func (r *rethinkMessage) Namespace() string {
	return r.namespace
}

func (r *rethinkMessage) OP() ops.Op {
	return r.op
}

func (r *rethinkMessage) ID() string {
	switch r := r.d["id"].(type) {
	case string:
		return r
	default:
		return fmt.Sprintf("%v", r)
	}
}
