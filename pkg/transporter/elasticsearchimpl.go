package transporter

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	elastigo "github.com/mattbaird/elastigo/lib"
)

type ElasticsearchImpl struct {
	// pull these in from the node
	uri  *url.URL
	role NodeRole

	_type string
	index string

	pipe pipe.Pipe

	indexer *elastigo.BulkIndexer
	running bool
}

func NewElasticsearchImpl(role NodeRole, extra map[string]interface{}) (*ElasticsearchImpl, error) {
	u, err := url.Parse(extra["uri"].(string))
	if err != nil {
		return nil, err
	}

	e := &ElasticsearchImpl{
		uri:  u,
		role: role,
	}

	e.index, e._type, err = e.splitNamespace(extra["namespace"].(string))
	if err != nil {
		return e, err
	}

	return e, nil
}

/*
 * start the module
 */
func (e *ElasticsearchImpl) Start(pipe pipe.Pipe) error {
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

func (e *ElasticsearchImpl) applyOp(msg *message.Msg) (*message.Msg, error) {
	if msg.Op == message.Command {
		return msg, e.runCommand(msg)
	}

	return msg, e.indexer.Index(e.index, e._type, msg.IdAsString(), "", nil, msg.Document(), false)
}

func (e *ElasticsearchImpl) setupClient() {
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

func (e *ElasticsearchImpl) getNamespace() string {
	return strings.Join([]string{e.index, e._type}, ".")
}

/*
 * split a elasticsearch namespace into a index and a type
 */
func (e *ElasticsearchImpl) splitNamespace(namespace string) (string, string, error) {
	fields := strings.SplitN(namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed mongo namespace.")
	}
	return fields[0], fields[1], nil
}
