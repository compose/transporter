package etcd

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

var (
	_ adaptor.Describable = &Etcd{}
)

// Etcd is an adaptor to read / write to Etcd.
// it works as a source by copying files, and then optionally watching all keys
type Etcd struct {
	client client.Client
	reader client.Reader
	writer client.Writer

	// pull these in from the node
	tail bool // run the tail oplog

	// save time by setting these once
	rootKey     string
	subKeyMatch *regexp.Regexp

	pipe *pipe.Pipe
	path string

	doneChannel chan struct{}
	wg          sync.WaitGroup
}

func init() {
	adaptor.Add("etcd", func(pipe *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
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
			pipe:        pipe,
			tail:        conf.Tail,
			path:        path,
			doneChannel: make(chan struct{}),
		}

		e.rootKey, e.subKeyMatch, err = extra.CompileNamespace()
		if err != nil {
			return nil, err
		}

		e.client, err = NewClient(
			WithURI(conf.URI),
			WithTimeout(conf.Timeout),
		)
		if err != nil {
			return nil, err
		}

		e.reader = newReader(e.rootKey)
		e.writer = newWriter(e.rootKey)

		return e, nil
	})
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

// Connect fulfills the Connectable interface
func (e *Etcd) Connect() error {
	_, err := e.client.Connect()
	return err
}

// Start the adaptor as a source
func (e *Etcd) Start() (err error) {
	log.With("path", e.path).Infoln("adaptor Starting...")
	defer func() {
		e.pipe.Stop()
	}()

	s, err := e.client.Connect()
	if err != nil {
		return err
	}
	readFunc := e.reader.Read(e.keyFilter)
	msgChan, err := readFunc(s, e.doneChannel)
	if err != nil {
		return err
	}
	for msg := range msgChan {
		e.pipe.Send(msg)
	}

	log.With("path", e.path).Infoln("adaptor Start finished...")
	return nil
}

// Listen starts the pipe's listener
func (e *Etcd) Listen() (err error) {
	log.With("path", e.path).Infoln("adaptor Listening...")
	defer func() {
		log.With("path", e.path).Infoln("adaptor Listen closing...")
		e.pipe.Stop()
	}()

	return e.pipe.Listen(e.writeMessage, e.subKeyMatch)
}

// Stop the adaptor
func (e *Etcd) Stop() error {
	log.With("path", e.path).Infoln("adaptor Stopping...")
	e.pipe.Stop()

	close(e.doneChannel)
	e.wg.Wait()

	log.With("path", e.path).Infoln("adaptor Stopped")
	return nil
}

func (e *Etcd) keyFilter(key string) bool {
	return e.subKeyMatch.MatchString(key)
}

// writeMessage writes one message to the destination Postgres, or sends an error down the pipe
// TODO this can be cleaned up.  I'm not sure whether this should pipe the error, or whether the
// caller should pipe the error
func (e *Etcd) writeMessage(msg message.Msg) (message.Msg, error) {
	_, ns, _ := message.SplitNamespace(msg)
	err := client.Write(e.client, e.writer, message.From(msg.OP(), fmt.Sprintf(".%s", ns), msg.Data()))

	if err != nil {
		e.pipe.Err <- adaptor.NewError(adaptor.ERROR, e.path, fmt.Sprintf("write message error (%s)", err), msg.Data)
	}
	return msg, err
}

// Config provides configuration options for a postgres adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type Config struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form TODO"`
	Namespace string `json:"namespace" doc:"etcd namespace to read/write"`
	Timeout   string `json:"timeout" doc:"timeout for sending requests, format must be parsable by time.ParseDuration and defaults to 5s"`
	Tail      bool   `json:"tail" doc:"if tail is true, then the etcd source will track changes after copying the namespace"`
}
