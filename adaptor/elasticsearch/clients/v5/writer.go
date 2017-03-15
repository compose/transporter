package v5

import (
	"context"
	"fmt"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/compose/transporter/adaptor/elasticsearch/clients"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	version "github.com/hashicorp/go-version"
)

var (
	_ client.Writer = &Writer{}
	_ client.Closer = &Writer{}
)

// Writer implements client.Writer and client.Session for sending requests to an elasticsearch
// cluster via its _bulk API.
type Writer struct {
	index  string
	bp     *elastic.BulkProcessor
	logger log.Logger
}

func init() {
	constraint, _ := version.NewConstraint(">= 5.0")
	clients.Add("v5", constraint, func(opts *clients.ClientOptions) (client.Writer, error) {
		esOptions := []elastic.ClientOptionFunc{
			elastic.SetURL(opts.URLs...),
			elastic.SetSniff(false),
			elastic.SetHttpClient(opts.HTTPClient),
			elastic.SetMaxRetries(2),
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
		w := &Writer{
			index:  opts.Index,
			logger: log.With("writer", "elasticsearch").With("version", 5),
		}
		p, err := esClient.BulkProcessor().
			Name("TransporterWorker-1").
			Workers(2).
			BulkActions(1000).               // commit if # requests >= 1000
			BulkSize(2 << 20).               // commit if size of requests >= 2 MB
			FlushInterval(30 * time.Second). // commit every 30s
			After(w.postBulkProcessor).
			Do(context.TODO())
		if err != nil {
			return nil, err
		}
		w.bp = p
		return w, nil
	})
}

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		indexType := msg.Namespace()
		var id string
		if _, ok := msg.Data()["_id"]; ok {
			id = msg.ID()
			msg.Data().Delete("_id")
		}

		var br elastic.BulkableRequest
		switch msg.OP() {
		case ops.Delete:
			// we need to flush any pending writes here or this could fail because we're using
			// more than 1 worker
			w.bp.Flush()
			br = elastic.NewBulkDeleteRequest().Index(w.index).Type(indexType).Id(id)
		case ops.Insert:
			br = elastic.NewBulkIndexRequest().Index(w.index).Type(indexType).Id(id).Doc(msg.Data())
		case ops.Update:
			br = elastic.NewBulkUpdateRequest().Index(w.index).Type(indexType).Id(id).Doc(msg.Data())
		}
		w.bp.Add(br)
		return msg, nil
	}
}

// Close is called by clients.Close() when it receives on the done channel.
func (w *Writer) Close() {
	w.logger.Infoln("closing BulkProcessor")
	w.bp.Close()
}

func (w *Writer) postBulkProcessor(executionID int64, reqs []elastic.BulkableRequest, resp *elastic.BulkResponse, err error) {
	if resp != nil && err == nil {
		w.logger.With("executionID", executionID).
			With("took", fmt.Sprintf("%dms", resp.Took)).
			With("succeeeded", len(resp.Succeeded())).
			With("failed", len(resp.Failed())).
			Infoln("_bulk flush completed")
	}
	if err != nil {
		w.logger.With("executionID", executionID).Errorln(err)
	}
}
