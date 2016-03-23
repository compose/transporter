package elasticsearch

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	elastigo "github.com/mattbaird/elastigo/lib"
)

// ElasticSearch is an adaptor to connect a pipeline to
// an elasticsearch cluster.
type ElasticSearch struct {
	// pull these in from the node
	uri *url.URL

	index     string
	typeMatch *regexp.Regexp

	pipe *pipe.Pipe
	path string

	indexer *elastigo.BulkIndexer
	running bool
}

// Description for the ElasticSearch adaptor
func (e *ElasticSearch) Description() string {
	return "an elasticsearch sink adaptor"
}

var sampleConfig = `
- es:
		type: elasticsearch
    uri: https://username:password@hostname:port/thisgetsignored
`

// SampleConfig for elasticsearch adaptor
func (e *ElasticSearch) SampleConfig() string {
	return sampleConfig
}

func init() {
	adaptor.Add("elasticsearch", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.StopStartListener, error) {
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

		e := &ElasticSearch{
			uri:  u,
			pipe: p,
		}

		e.index, e.typeMatch, err = extra.CompileNamespace()
		if err != nil {
			return e, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("can't split namespace into _index and typeMatch (%s)", err.Error()), nil)
		}

		return e, nil
	})
}

// Connect is a no-op for Elasticsearch adaptors
func (e *ElasticSearch) Connect() error {
	return nil
}

// Start the adaptor as a source (not implemented)
func (e *ElasticSearch) Start() error {
	return fmt.Errorf("elasticsearch can't function as a source")
}

// Listen starts the listener
func (e *ElasticSearch) Listen() error {
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
func (e *ElasticSearch) Stop() error {
	if e.running {
		e.running = false
		e.pipe.Stop()
		e.indexer.Stop()
	}
	return nil
}

func (e *ElasticSearch) applyOp(msg *message.Msg) (*message.Msg, error) {
	if msg.Op == message.Command {
		err := e.runCommand(msg)
		if err != nil {
			e.pipe.Err <- adaptor.NewError(adaptor.ERROR, e.path, fmt.Sprintf("elasticsearch error (%s)", err), msg.Data)
		}
		return msg, nil
	}

	// TODO there might be some inconsistency here.  elasticsearch uses the _id field for an primary index,
	//  and we're just mapping it to a string here.
	id, err := msg.IDString("_id")
	if err != nil {
		id = ""
	}

	_, _type, err := msg.SplitNamespace()
	if err != nil {
		e.pipe.Err <- adaptor.NewError(adaptor.ERROR, e.path, fmt.Sprintf("unable to determine type from msg.Namespace (%s)", msg.Namespace), msg)
		return msg, nil
	}
	switch msg.Op {
	case message.Delete:
		e.indexer.Delete(e.index, _type, id, false)
		err = nil
	default:
		err = e.indexer.Index(e.index, _type, id, "", "", nil, msg.Data, false)
	}
	if err != nil {
		e.pipe.Err <- adaptor.NewError(adaptor.ERROR, e.path, fmt.Sprintf("elasticsearch error (%s)", err), msg.Data)
	}
	return msg, nil
}

func (e *ElasticSearch) setupClient() {
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

func (e *ElasticSearch) runCommand(msg *message.Msg) error {
	if !msg.IsMap() {
		return nil
	}

	if _, hasKey := msg.Map()["flush"]; hasKey {
		e.indexer.Flush()
	}
	return nil
}
