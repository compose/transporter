package elasticsearch

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/adaptor/elasticsearch"
	"github.com/compose/transporter/pkg/pipe"
	elastigo "github.com/mattbaird/elastigo/lib"
)

// Elasticsearch is an adaptor to connect a pipeline to
// an elasticsearch cluster.
type Elasticsearch struct {
	// pull these in from the node
	uri *url.URL

	index     string
	typeMatch *regexp.Regexp

	pipe *pipe.Pipe
	path string

	indexer *elastigo.BulkIndexer
	running bool
}

// Description for the Elasticsearcb adaptor
func (e *Elasticsearch) Description() string {
	return "an elasticsearch sink adaptor"
}

const sampleConfig = `
- es:
		type: elasticsearch
    uri: https://username:password@hostname:port/thisgetsignored
`

// SampleConfig for elasticsearch adaptor
func (e *Elasticsearch) SampleConfig() string {
	return sampleConfig
}

func init() {
	adaptor.Add("elasticsearch", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf adaptor.DbConfig
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("bad config (%s)", err.Error()), nil)
		}

		u, err := url.Parse(conf.URI)
		if err != nil {
			return nil, err
		}

		e := &Elasticsearch{
			uri:  u,
			pipe: p,
			path: path,
		}

		e.index, e.typeMatch, err = extra.CompileNamespace()
		if err != nil {
			return e, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("can't split namespace into _index and typeMatch (%s)", err.Error()), nil)
		}

		return e, nil
	})
}

// Start the adaptor as a source (not implemented)
func (e *Elasticsearch) Start() error {
	return fmt.Errorf("elasticsearch can't function as a source")
}

// Listen starts the listener
func (e *Elasticsearch) Listen() error {
	e.setupClient()
	e.indexer.Start()
	e.running = true

	go func(cherr chan *elastigo.ErrorBuffer) {
		for err := range e.indexer.ErrorChannel {
			e.pipe.Err <- adaptor.NewError(adaptor.CRITICAL, e.path, fmt.Sprintf("elasticsearch error (%s)", err.Err), nil)
		}
	}(e.indexer.ErrorChannel)

	defer func() {
		if e.running {
			e.running = false
			e.pipe.Stop()
			e.indexer.Stop()
		}
	}()

	return e.pipe.Listen(e.applyOp, e.typeMatch)
}

// Stop the adaptor
func (e *Elasticsearch) Stop() error {
	if e.running {
		e.running = false
		e.pipe.Stop()
		e.indexer.Stop()
	}
	return nil
}

func (e *Elasticsearch) applyOp(msg message.Msg) (message.Msg, error) {
	m, err := message.Exec(message.MustUseAdaptor("elasticsearch").(elasticsearch.Adaptor).UseIndexer(e.indexer).UseIndex(e.index), msg)
	if err != nil {
		e.pipe.Err <- adaptor.NewError(adaptor.ERROR, e.path, fmt.Sprintf("elasticsearch error (%s)", err), msg.Data)
	}
	return m, err
}

func (e *Elasticsearch) setupClient() {
	// set up the client, we need host(s), port, username, password, and scheme
	client := elastigo.NewConn()

	if e.uri.User != nil {
		client.Username = e.uri.User.Username()
		if password, set := e.uri.User.Password(); set {
			client.Password = password
		}
	}

	// we might have a port in the host bit
	hostBits := strings.Split(e.uri.Host, ":")
	if len(hostBits) > 1 {
		client.SetPort(hostBits[1])
	}

	client.SetHosts(strings.Split(hostBits[0], ","))
	client.Protocol = e.uri.Scheme

	e.indexer = client.NewBulkIndexerErrors(10, 60)
}
