package rethinkdb

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
)

const (
	sampleConfig = `
	- rethink:
	    type: rethinkdb
	    uri: rethink://127.0.0.1:28015
			# timeout: 30s
      # tail: false
      # ssl: false
      # cacerts: ["/path/to/cert.pem"]
	`

	description = "a rethinkdb adaptor that functions as both a source and a sink"
)

// Config provides custom configuration options for the RethinkDB adapter
type Config struct {
	URI       string   `json:"uri" doc:"the uri to connect to, in the form rethink://user:password@host.example:28015/database"`
	Namespace string   `json:"namespace" doc:"rethink namespace to read/write"`
	Timeout   string   `json:"timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	Tail      bool     `json:"tail" doc:"if true, the RethinkDB table will be monitored for changes after copying the namespace"`
	SSL       bool     `json:"ssl" doc:"enable TLS connection"`
	CACerts   []string `json:"cacerts" doc:"array of root CAs to use in order to verify the server certificates"`
}

// RethinkDB is an adaptor that writes metrics to rethinkdb (http://rethinkdb.com/)
// An open-source distributed database
type RethinkDB struct {
	client client.Client
	reader client.Reader
	writer client.Writer

	tableMatch *regexp.Regexp

	pipe *pipe.Pipe
	path string

	doneChannel chan struct{}
	wg          sync.WaitGroup
}

func init() {
	adaptor.Add("rethinkdb", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf Config
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, err
		}

		if conf.URI == "" || conf.Namespace == "" {
			return nil, fmt.Errorf("both uri and namespace required, but missing")
		}
		log.With("path", path).Debugf("adaptor config: %+v", conf)

		db, tableMatch, err := extra.CompileNamespace()
		if err != nil {
			return nil, err
		}
		log.With("path", path).Debugf("tableMatch: %+v", tableMatch)

		r := &RethinkDB{
			pipe:        p,
			path:        path,
			reader:      newReader(db, conf.Tail),
			tableMatch:  tableMatch,
			doneChannel: make(chan struct{}),
		}

		r.client, err = NewClient(
			WithURI(conf.URI),
			WithDatabase(db),
			WithSessionTimeout(conf.Timeout),
			WithSSL(conf.SSL),
			WithCACerts(conf.CACerts),
		)
		if err != nil {
			return nil, err
		}

		return r, nil
	})
}

// Description for rethinkdb adaptor
func (r *RethinkDB) Description() string {
	return description
}

// SampleConfig for rethinkdb adaptor
func (r *RethinkDB) SampleConfig() string {
	return sampleConfig
}

// Connect tests the connection and if successful, connects to the database
func (r *RethinkDB) Connect() error {
	_, err := r.client.Connect()
	return err
}

// Start the adaptor as a source
func (r *RethinkDB) Start() error {
	log.With("path", r.path).Infoln("adaptor Starting...")
	defer func() {
		r.pipe.Stop()
	}()

	s, err := r.client.Connect()
	if err != nil {
		return err
	}
	readFunc := r.reader.Read(r.tableFilter)
	msgChan, err := readFunc(s, r.doneChannel)
	if err != nil {
		return err
	}
	for msg := range msgChan {
		r.pipe.Send(msg)
	}

	log.With("path", r.path).Infoln("adaptor Start finished...")
	return nil
}

// Listen start's the adaptor's listener
func (r *RethinkDB) Listen() (err error) {
	return r.pipe.Listen(r.applyOp, r.tableMatch)
}

// Stop the adaptor
func (r *RethinkDB) Stop() error {
	log.With("path", r.path).Infoln("adaptor Stopping...")
	r.pipe.Stop()

	close(r.doneChannel)
	r.wg.Wait()

	if c, ok := r.client.(client.Closer); ok {
		c.Close()
	}

	log.With("path", r.path).Infoln("adaptor Stopped")
	return nil
}

// applyOp applies one operation to the database
func (r *RethinkDB) applyOp(msg message.Msg) (message.Msg, error) {
	err := client.Write(r.client, r.writer, message.From(msg.OP(), msg.Namespace(), msg.Data()))

	if err != nil {
		r.pipe.Err <- adaptor.NewError(adaptor.ERROR, r.path, fmt.Sprintf("write message error (%s)", err), msg.Data)
	}
	return msg, err
}

func (r *RethinkDB) tableFilter(table string) bool {
	return r.tableMatch.MatchString(table)
}
