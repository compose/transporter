package node

import (
	"net/url"
	"strings"

	"github.com/compose/transporter/pkg/message"
	elastigo "github.com/mattbaird/elastigo/lib"
)

type ElasticsearchImpl struct {
	uri *url.URL

	_type string
	index string

	config ConfigNode

	pipe Pipe

	// client  *elastigo.Conn
	indexer *elastigo.BulkIndexer
	running bool
}

func NewElasticsearchImpl(c ConfigNode) (*ElasticsearchImpl, error) {
	u, err := url.Parse(c.Uri)
	if err != nil {
		return nil, err
	}
	return &ElasticsearchImpl{uri: u, config: c}, nil
}

/*
 * start the module
 */
func (e *ElasticsearchImpl) Start(pipe Pipe) error {
	e.pipe = pipe
	e.setupClient()
	e.indexer.Start()
	e.running = true

	go func(cherr chan *elastigo.ErrorBuffer) {
		for err := range e.indexer.ErrorChannel {
			e.pipe.Err <- err.Err
		}
	}(e.indexer.ErrorChannel)

	defer func() {
		if e.running {
			e.running = false
			e.pipe.Stop()
			e.indexer.Stop()
		}
	}()

	return e.pipe.Listen(e.applyOp)
}

/*
 * stop the capsule
 */
func (e *ElasticsearchImpl) Stop() error {
	if e.running {
		e.running = false
		e.pipe.Stop()
		e.indexer.Stop()
	}
	return nil
}

func (e *ElasticsearchImpl) Config() ConfigNode {
	return e.config
}

func (e *ElasticsearchImpl) applyOp(msg *message.Msg) (err error) {
	if msg.Op == message.Command {
		return e.runCommand(msg)
	}

	return e.indexer.Index(e.index, e._type, msg.IdAsString(), "", nil, msg.Document(), false)
}

func (e *ElasticsearchImpl) setupClient() {
	// split the namespace into the index and type
	fields := strings.SplitN(e.config.Namespace, ".", 2)
	e.index, e._type = fields[0], fields[1]

	// set up the client, we need host(s), port, username, password, and scheme
	client := elastigo.NewConn()

	if e.uri.User != nil {
		client.Username = e.uri.User.Username()
		if password, set := e.uri.User.Password(); set {
			client.Password = password
		}
	}

	// we might have a port in the host bit
	host_bits := strings.Split(e.uri.Host, ":")
	if len(host_bits) > 1 {
		client.SetPort(host_bits[1])
	}

	client.SetHosts(strings.Split(host_bits[0], ","))
	client.Protocol = e.uri.Scheme

	e.indexer = client.NewBulkIndexerErrors(10, 60)
}

func (e *ElasticsearchImpl) runCommand(msg *message.Msg) error {
	if _, has_key := msg.Document()["flush"]; has_key {
		e.indexer.Flush()
	}
	return nil
}
