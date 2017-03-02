package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/pipe"

	_ "github.com/lib/pq" // import pq driver
)

const (
	description = "a postgres adaptor that functions as both a source and a sink"

	sampleConfig = `    type: postgres
    uri: ${POSTGRESQL_URI}
    # debug: false
    # tail: false
    # replication_slot: slot`
)

// Postgres is an adaptor to read / write to postgres.
// it works as a source by copying files, and then optionally tailing the oplog
type Postgres struct {
	// pull these in from the node
	uri             string
	tail            bool   // run the tail oplog
	replicationSlot string // logical replication slot to use for changes
	debug           bool

	conf   Config
	client client.Client
	reader client.Reader
	writer client.Writer

	// save time by setting these once
	tableMatch *regexp.Regexp
	database   string

	latestLSN string

	//
	pipe *pipe.Pipe
	path string

	doneChannel chan struct{}
}

// Config provides configuration options for a postgres adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type Config struct {
	URI             string `json:"uri" doc:"the uri to connect to, in the form 'user=my-user password=my-password dbname=dbname sslmode=require'"`
	Namespace       string `json:"namespace" doc:"mongo namespace to read/write"`
	Debug           bool   `json:"debug" doc:"display debug information"`
	Tail            bool   `json:"tail" doc:"if tail is true, then the postgres source will tail the oplog after copying the namespace"`
	ReplicationSlot string `json:"replication_slot" doc:"required if tail is true; sets the replication slot to use for logical decoding"`
}

func init() {
	adaptor.Add("postgres", func(ppipe *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf Config
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, err
		}

		if conf.URI == "" || conf.Namespace == "" {
			return nil, fmt.Errorf("both uri and namespace required, but missing ")
		}

		log.With("path", path).Debugf("adaptor config: %+v", conf)

		db, tableMatch, err := extra.CompileNamespace()
		if err != nil {
			return nil, err
		}

		p := &Postgres{
			reader:      newReader(db),
			writer:      newWriter(db),
			tableMatch:  tableMatch,
			pipe:        ppipe,
			path:        path,
			doneChannel: make(chan struct{}),
		}

		p.client, err = NewClient(WithURI(conf.URI))
		if err != nil {
			return p, err
		}

		if conf.Tail {
			p.reader = newTailer(db, conf.ReplicationSlot)
		}
		return p, nil
	})
}

// Description for postgres adaptor
func (p *Postgres) Description() string {
	return description
}

// SampleConfig for postgres adaptor
func (p *Postgres) SampleConfig() string {
	return sampleConfig
}

// Connect tests the connection to postgres
func (p *Postgres) Connect() error {
	_, err := p.client.Connect()
	return err
}

// Start the adaptor as a source
func (p *Postgres) Start() (err error) {
	log.With("path", p.path).Infoln("adaptor Starting...")
	defer func() {
		p.pipe.Stop()
	}()

	s, err := p.client.Connect()
	if err != nil {
		return err
	}
	if c, ok := s.(client.Closer); ok {
		defer c.Close()
	}
	readFunc := p.reader.Read(p.matchFunc)
	msgChan, err := readFunc(s, p.doneChannel)
	if err != nil {
		return err
	}
	for msg := range msgChan {
		p.pipe.Send(msg)
	}

	log.With("path", p.path).Infoln("adaptor Start finished...")
	return nil
}

func (p *Postgres) matchFunc(table string) bool {
	if strings.HasPrefix(table, "information_schema.") || strings.HasPrefix(table, "pg_catalog.") {
		return false
	}
	return p.tableMatch.MatchString(table)
}

// Listen starts the pipe's listener
func (p *Postgres) Listen() (err error) {
	log.With("path", p.path).Infoln("adaptor Listening...")
	defer func() {
		log.With("path", p.path).Infoln("adaptor Listen closing...")
		p.pipe.Stop()
	}()

	return p.pipe.Listen(p.writeMessage, p.tableMatch)
}

// writeMessage writes one message to the destination postgres, or sends an error down the pipe
func (p *Postgres) writeMessage(msg message.Msg) (message.Msg, error) {
	err := client.Write(p.client, p.writer, message.From(msg.OP(), msg.Namespace(), msg.Data()))

	if err != nil {
		p.pipe.Err <- adaptor.Error{
			Lvl:    adaptor.ERROR,
			Path:   p.path,
			Err:    fmt.Sprintf("write message error (%s)", err),
			Record: msg.Data(),
		}
	}
	return msg, err
}

// Stop the adaptor
func (p *Postgres) Stop() error {
	p.pipe.Stop()
	if c, ok := p.client.(client.Closer); ok {
		c.Close()
	}
	return nil
}
