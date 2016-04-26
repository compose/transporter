package transformer

import (
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type Adaptor struct {
}

func init() {
	a := Adaptor{}
	message.Register(a.Name(), a)
}

func (r Adaptor) Name() string {
	return "transformer"
}

func (r Adaptor) From(op ops.Op, namespace string, d interface{}) message.Msg {
	m := &TransformerMessage{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
	}
	switch d.(type) {
	case map[string]interface{}:
		m.MapData = data.MapData(d.(map[string]interface{}))
	case bson.M:
		m.MapData = data.MapData(d.(bson.M))
	case data.MapData:
		m.MapData = d.(data.MapData)
	}
	return m
}
