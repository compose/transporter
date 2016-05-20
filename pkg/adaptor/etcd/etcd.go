package etcd

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/adaptor/etcd"
	"github.com/compose/transporter/pkg/pipe"
	"github.com/coreos/etcd/client"
)

// Etcd is an adaptor to read / write to Etcd.
// it works as a source by copying files, and then optionally watching all keys
type Etcd struct {
	// pull these in from the node
	uri  string
	tail bool // run the tail oplog

	// save time by setting these once
	tableMatch *regexp.Regexp
	database   string

	//
	pipe *pipe.Pipe
	path string

	// etcd connection and options
	session        client.Client
	sessionTimeout time.Duration
}

// Config provides configuration options for a postgres adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type Config struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form TODO"`
	Namespace string `json:"namespace" doc:"mongo namespace to read/write"`
	Timeout   string `json:"timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	Tail      bool   `json:"tail" doc:"if tail is true, then the etcd source will track changes after copying the namespace"`
}

func init() {
	adaptor.Add("etcd", adaptor.Creator(func(ppipe *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf Config
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, err
		}

		if conf.URI == "" || conf.Namespace == "" {
			return nil, fmt.Errorf("both endpoints and namespace required, but missing ")
		}
		e := &Etcd{
			sessionTimeout: time.Second * 10,
			pipe:           ppipe,
			uri:            conf.URI,
			tail:           conf.Tail,
			path:           path,
		}
		e.database, e.tableMatch, err = extra.CompileNamespace()
		if err != nil {
			return e, err
		}
		if conf.Timeout != "" {
			t, err := time.ParseDuration(conf.Timeout)
			if err != nil {
				log.Printf("error parsing timeout, defaulting to 10s, %v", err)
			} else {
				e.sessionTimeout = t
			}
		}
		return e, nil
	}))
}

// Description for etcd adaptor
func (e *Etcd) Description() string {
	return "an etcd adaptor that functions as both a source and a sink"
}

const sampleConfig = `
- localetcd:
    type: etcd
    uri: 127.0.0.1:5432,127.0.0.1:2345
`

// SampleConfig for postgres adaptor
func (e *Etcd) SampleConfig() string {
	return sampleConfig
}

func (e *Etcd) Connect() error {
	cfg := client.Config{
		Endpoints: strings.Split(e.uri, ","),
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second * 5,
	}
	var err error
	e.session, err = client.New(cfg)
	if err != nil {
		return fmt.Errorf("unable to make etcd session (%v), %v\n", e.uri, err)
	}
	return nil
}

// Start the adaptor as a source
func (e *Etcd) Start() (err error) {
	defer func() {
		e.pipe.Stop()
	}()

	err = e.catData()
	if err != nil {
		e.pipe.Err <- err
		return fmt.Errorf("Error connecting to Postgres: %v", err)
	}
	if e.tail {
		err = e.tailData()
		if err != nil {
			e.pipe.Err <- err
			return err
		}
	}
	return
}

// Listen starts the pipe's listener
func (e *Etcd) Listen() (err error) {
	defer func() {
		e.pipe.Stop()
	}()

	return e.pipe.Listen(e.writeMessage, e.tableMatch)
}

// Stop the adaptor
func (e *Etcd) Stop() error {
	e.pipe.Stop()
	return nil
}

// writeMessage writes one message to the destination Postgres, or sends an error down the pipe
// TODO this can be cleaned up.  I'm not sure whether this should pipe the error, or whether the
//   caller should pipe the error
func (e *Etcd) writeMessage(msg message.Msg) (message.Msg, error) {
	m, err := message.Exec(message.MustUseAdaptor("etcd").(etcd.Adaptor).UseClient(e.session), msg)
	if err != nil {
		e.pipe.Err <- adaptor.NewError(adaptor.ERROR, e.path, fmt.Sprintf("etcd error (%v)", err), msg.Data())
	}

	return m, err
}

// catdata pulls down the original tables
func (e *Etcd) catData() error {
	return nil
}

// tail the logical data
func (e *Etcd) tailData() error {
	return nil
}
