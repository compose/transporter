package elasticsearch

import (
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/adaptor/elasticsearch/clients"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/message/ops"
)

type Adaptor struct {
	// indexer *elastigo.BulkIndexer
	client *clients.Client
	index  string
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

func (r Adaptor) From(op ops.Op, namespace string, d data.Data) message.Msg {
	return &Message{
		Operation: op,
		TS:        time.Now().Unix(),
		NS:        namespace,
		MapData:   d,
	}
}

func (r Adaptor) Insert(m message.Msg) error {
	m.Data().Delete("_id")
	return r.Update(m)
}

func (r Adaptor) Delete(m message.Msg) error {
	_, _, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	// r.indexer.Delete(r.index, t, m.ID())
	return nil
}

func (r Adaptor) Update(m message.Msg) error {
	_, _, err := message.SplitNamespace(m)
	if err != nil {
		return err
	}
	// return r.indexer.Index(r.index, t, m.ID(), "", "", nil, m.Data())
	return nil
}

func (r Adaptor) Command(m message.Msg) error {
	// if _, hasKey := m.Data().Has("flush"); hasKey {
	// 	r.indexer.Flush()
	// }
	return nil
}

// func (r Adaptor) UseIndexer(indexer *elastigo.BulkIndexer) Adaptor {
// 	r.indexer = indexer
// 	return r
// }

func (r Adaptor) UseIndex(index string) Adaptor {
	r.index = index
	return r
}
