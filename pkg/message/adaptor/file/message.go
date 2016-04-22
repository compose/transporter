package file

import (
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type FileMessage struct {
	TS        int64
	MapData   data.MapData
	NS        string
	Operation ops.Op
}

func (f *FileMessage) Timestamp() int64 {
	return f.TS
}

func (f *FileMessage) Data() interface{} {
	return f.MapData
}

func (f *FileMessage) Namespace() string {
	return f.NS
}

func (f *FileMessage) OP() ops.Op {
	return f.Operation
}

func (f *FileMessage) ID() string {
	return ""
}
