package file

import (
	"git.compose.io/compose/transporter/pkg/message/data"
	"git.compose.io/compose/transporter/pkg/message/ops"
)

type Message struct {
	TS        int64
	MapData   data.Data
	NS        string
	Operation ops.Op
}

func (f *Message) Timestamp() int64 {
	return f.TS
}

func (f *Message) Data() data.Data {
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
