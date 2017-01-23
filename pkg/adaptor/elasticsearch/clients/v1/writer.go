package v1

import (
	"context"
	"sync"

	elastic "gopkg.in/olivere/elastic.v2"

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

// Writer implements client.Writer and client.Session for sending requests to an elasticsearch
// cluster in individual requests.
type Writer struct {
	esClient *elastic.Client
}

func init() {
	constraint, _ := version.NewConstraint(">= 1.4, < 2.0")
	clients.Add("v1", constraint, func(done chan struct{}, wg *sync.WaitGroup, opts *clients.ClientOptions) (client.Writer, error) {
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
	w := &Writer{esClient: client}
	wg.Add(1)
	go clients.Close(done, wg, w)
	return w
}

func (w *Writer) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		i, t, _ := message.SplitNamespace(msg)
		var id string
		if _, ok := msg.Data()["_id"]; ok {
			id = msg.ID()
		}

		switch msg.OP() {
		case ops.Delete:
			w.esClient.Delete().Index(i).Type(t).Id(id).Do(context.TODO())
		case ops.Insert:
			w.esClient.Index().Index(i).Type(t).Id(id).BodyJson(msg.Data()).Do(context.TODO())
		case ops.Update:
			w.esClient.Index().Index(i).Type(t).BodyJson(msg.Data()).Id(id).Do(context.TODO())
		}
		return nil
	}
}

// Close is called by clients.Close() when it receives on the done channel.
func (w *Writer) Close() {
	// no op
}
