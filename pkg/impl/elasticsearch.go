package impl

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	elastigo "github.com/mattbaird/elastigo/lib"
)

type Elasticsearch struct {
	// pull these in from the node
	uri *url.URL

	_type string
	index string

	pipe pipe.Pipe

	indexer *elastigo.BulkIndexer
	running bool
}

func NewElasticsearch(p pipe.Pipe, extra ExtraConfig) (*Elasticsearch, error) {
	var (
		conf ElasticsearchConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	u, err := url.Parse(conf.Uri)
	if err != nil {
		return nil, err
	}

	e := &Elasticsearch{
		uri:  u,
		pipe: p,
	}

	e.index, e._type, err = e.splitNamespace(conf.Namespace)
	if err != nil {
		return e, err
	}

	return e, nil
}

// start the listener
func (e *Elasticsearch) Listen() error {
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
func (e *Elasticsearch) Stop() error {
	if e.running {
		e.running = false
		e.pipe.Stop()
		e.indexer.Stop()
	}
	return nil
}

func (e *Elasticsearch) applyOp(msg *message.Msg) (*message.Msg, error) {
	if msg.Op == message.Command {
		return msg, e.runCommand(msg)
	}

	return msg, e.indexer.Index(e.index, e._type, msg.IdAsString(), "", nil, msg.Document(), false)
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
	host_bits := strings.Split(e.uri.Host, ":")
	if len(host_bits) > 1 {
		client.SetPort(host_bits[1])
	}

	client.SetHosts(strings.Split(host_bits[0], ","))
	client.Protocol = e.uri.Scheme

	e.indexer = client.NewBulkIndexerErrors(10, 60)
}

func (e *Elasticsearch) runCommand(msg *message.Msg) error {
	if _, has_key := msg.Document()["flush"]; has_key {
		e.indexer.Flush()
	}
	return nil
}

func (e *Elasticsearch) getNamespace() string {
	return strings.Join([]string{e.index, e._type}, ".")
}

/*
 * split a elasticsearch namespace into a index and a type
 */
func (e *Elasticsearch) splitNamespace(namespace string) (string, string, error) {
	fields := strings.SplitN(namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed elasticsearch namespace.")
	}
	return fields[0], fields[1], nil
}

// ElasticsearchConfig options
type ElasticsearchConfig struct {
	Uri       string `json:"uri"`       // the database uri
	Namespace string `json:"namespace"` // namespace
	Debug     bool   `json:"debug"`     // debug mode
}
