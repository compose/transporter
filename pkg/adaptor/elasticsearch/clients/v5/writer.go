package v5

import (
	"sync"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/compose/transporter/pkg/adaptor/elasticsearch/clients"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
	version "github.com/hashicorp/go-version"
)

var (
	_ client.Writer  = &Writer{}
	_ client.Session = &Writer{}
)

type Writer struct {
	bp *elastic.BulkProcessor
}

func init() {
	constraint, _ := version.NewConstraint(">= 5.0")
	clients.Add("v5", constraint, func(done chan struct{}, wg *sync.WaitGroup, opts *clients.ClientOptions) (client.Writer, error) {
		esOptions := []elastic.ClientOptionFunc{
			elastic.SetURL(opts.URLs...),
			elastic.SetSniff(false),
			elastic.SetHttpClient(opts.HTTPClient),
			elastic.SetMaxRetries(2),
			elastic.SetInfoLog(log.Base().With("path", opts.Path)),
		}
		if opts.UserInfo != nil {
			if pwd, ok := opts.UserInfo.Password(); ok {
				esOptions = append(esOptions, elastic.SetBasicAuth(opts.UserInfo.Username(), pwd))
			}
		}
		esClient, err := elastic.NewClient(esOptions...)
		if err != nil {
			return nil, err
		}
		return newWriter(esClient, done, wg), nil
	})
}

func newWriter(client *elastic.Client, done chan struct{}, wg *sync.WaitGroup) *Writer {
	p, _ := client.BulkProcessor().
		Name("TransporterWorker-1").
		Workers(2).
		BulkActions(1000).               // commit if # requests >= 1000
		BulkSize(2 << 20).               // commit if size of requests >= 2 MB
		FlushInterval(30 * time.Second). // commit every 30s
		Do()
	w := &Writer{bp: p}
	wg.Add(1)
	go clients.Closer(done, wg, w)
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		i, t, _ := message.SplitNamespace(msg)
		var id string
		if _, ok := msg.Data()["_id"]; ok {
			id = msg.ID()
			msg.Data().Delete("_id")
		}

		var br elastic.BulkableRequest
		switch msg.OP() {
		case ops.Delete:
			br = elastic.NewBulkDeleteRequest().Index(i).Type(t).Id(id)
		case ops.Insert:
			br = elastic.NewBulkIndexRequest().Index(i).Type(t).Id(id).Doc(msg.Data())
		case ops.Update:
			br = elastic.NewBulkUpdateRequest().Index(i).Type(t).Id(id).Doc(msg.Data())
		}
		w.bp.Add(br)
		return nil
	}
}

func (w *Writer) Close() {
	w.bp.Close()
}
