package file

import (
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type fileMessage struct {
	ts        int64
	d         data.MapData
	namespace string
	op        ops.Op
}

func (f *fileMessage) Timestamp() int64 {
	return f.ts
}

func (f *fileMessage) Data() interface{} {
	return f.d
}

func (f *fileMessage) Namespace() string {
	return f.namespace
}

func (f *fileMessage) OP() ops.Op {
	return f.op
}

func (f *fileMessage) ID() string {
	return ""
}
