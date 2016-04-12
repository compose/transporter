package elasticsearch

import (
	"fmt"

	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type elasticsearchMessage struct {
	ts        int64
	d         data.MapData
	namespace string
	op        ops.Op
}

func (r *elasticsearchMessage) Timestamp() int64 {
	return r.ts
}

func (r *elasticsearchMessage) Data() interface{} {
	return r.d
}

func (r *elasticsearchMessage) Namespace() string {
	return r.namespace
}

func (r *elasticsearchMessage) OP() ops.Op {
	return r.op
}

func (r *elasticsearchMessage) ID() string {
	switch r := r.d["_id"].(type) {
	case string:
		return r
	default:
		return fmt.Sprintf("%v", r)
	}
}
