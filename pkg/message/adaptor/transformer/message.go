package transformer

import (
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type TransformerMessage struct {
	TS        int64
	MapData   data.MapData
	NS        string
	Operation ops.Op
}

func (r *TransformerMessage) Timestamp() int64 {
	return r.TS
}

func (r *TransformerMessage) Data() interface{} {
	return r.MapData
}

func (r *TransformerMessage) Namespace() string {
	return r.NS
}

func (r *TransformerMessage) OP() ops.Op {
	return r.Operation
}

func (r *TransformerMessage) ID() string {
	return ""
}
