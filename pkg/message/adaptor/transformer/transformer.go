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

var _ message.Adaptor = Adaptor{}
var _ message.Insertable = Adaptor{}
var _ message.Commandable = Adaptor{}
var _ message.Deletable = Adaptor{}
var _ message.Updatable = Adaptor{}

func init() {
	a := Adaptor{}
	message.Register(a.Name(), a)
}

func (r Adaptor) Name() string {
	return "rethinkdb"
}

func (r Adaptor) From(op ops.Op, namespace string, d interface{}) message.Msg {
	m := &TransformerMessage{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
	}
	switch d.(type) {
	case map[string]interface{}, bson.M:
		m.MapData = data.MapData(d.(map[string]interface{}))
	}
	return m
}

func (r Adaptor) Insert(m message.Msg) error {
	return nil
}

func (r Adaptor) Delete(m message.Msg) error {
	return nil
}

func (r Adaptor) Update(m message.Msg) error {
	return nil
}

func (r Adaptor) Command(m message.Msg) error {
	return nil
}
