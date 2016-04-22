package elasticsearch

import (
	"fmt"

	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type ElasticsearchMessage struct {
	TS        int64
	MapData   data.MapData
	NS        string
	Operation ops.Op
}

func (r *ElasticsearchMessage) Timestamp() int64 {
	return r.TS
}

func (r *ElasticsearchMessage) Data() interface{} {
	return r.MapData
}

func (r *ElasticsearchMessage) Namespace() string {
	return r.NS
}

func (r *ElasticsearchMessage) OP() ops.Op {
	return r.Operation
}

func (r *ElasticsearchMessage) ID() string {
	switch r := r.MapData["_id"].(type) {
	case string:
		return r
	default:
		return fmt.Sprintf("%v", r)
	}
}
