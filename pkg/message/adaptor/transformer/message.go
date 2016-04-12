package transformer

import (
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type transformerMessage struct {
	ts        int64
	d         data.MapData
	namespace string
	op        ops.Op
}

func (r *transformerMessage) Timestamp() int64 {
	return r.ts
}

func (r *transformerMessage) Data() interface{} {
	return r.d
}

func (r *transformerMessage) Namespace() string {
	return r.namespace
}

func (r *transformerMessage) OP() ops.Op {
	return r.op
}

func (r *transformerMessage) ID() string {
	return ""
}
