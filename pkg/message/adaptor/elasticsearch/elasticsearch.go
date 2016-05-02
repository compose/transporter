package elasticsearch

import (
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
	"github.com/mattbaird/elastigo/lib"
)

type Adaptor struct {
	indexer *elastigo.BulkIndexer
	index   string
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
	return "elasticsearch"
}

func (r Adaptor) From(op ops.Op, namespace string, d interface{}) message.Msg {
	m := &Message{
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

func (r Adaptor) Insert(m message.Msg) error {
	delete(m.(*Message).MapData, "_id")
	return r.Update(m)
}

func (r Adaptor) Delete(m message.Msg) error {
	_, t, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	r.indexer.Delete(r.index, t, m.ID(), false)
	return nil
}

func (r Adaptor) Update(m message.Msg) error {
	_, t, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	return r.indexer.Index(r.index, t, m.ID(), "", "", nil, m.Data(), false)
}

func (r Adaptor) Command(m message.Msg) error {
	if _, hasKey := m.Data().(data.MapData)["flush"]; hasKey {
		r.indexer.Flush()
	}
	return nil
}

func (r Adaptor) UseIndexer(indexer *elastigo.BulkIndexer) Adaptor {
	r.indexer = indexer
	return r
}

func (r Adaptor) UseIndex(index string) Adaptor {
	r.index = index
	return r
}
