package file

import (
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type Message struct {
	TS        int64
	MapData   data.MapData
	NS        string
	Operation ops.Op
}

func (f *Message) Timestamp() int64 {
	return f.TS
}

func (f *Message) Data() interface{} {
	return f.MapData
}

func (f *Message) Namespace() string {
	return f.NS
}

func (f *Message) OP() ops.Op {
	return f.Operation
}

func (f *Message) ID() string {
	return ""
}
