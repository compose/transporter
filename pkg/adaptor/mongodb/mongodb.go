package mongodb

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/data"
	"github.com/compose/transporter/pkg/pipe"
)

const (
	description = "a mongodb adaptor that functions as both a source and a sink"

	sampleConfig = `
	- localmongo:
	    type: mongodb
	    uri: mongodb://127.0.0.1:27017/test
	`
)

// MongoDB is an adaptor to read / write to mongodb.
// it works as a source by copying files, and then optionally tailing the oplog
type MongoDB struct {
	// pull these in from the node
	conf   Config
	client client.Client
	writer client.Writer
	reader client.Reader

	// save time by setting these once
	collectionMatch *regexp.Regexp
	database        string

	pipe *pipe.Pipe
	path string

	doneChannel chan struct{}
	wg          sync.WaitGroup
}

type syncDoc struct {
	Doc        data.Data
	Collection string
}

func init() {
	adaptor.Add("mongodb", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
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

		db, collectionMatch, err := extra.CompileNamespace()
		if err != nil {
			return nil, err
		}

		m := &MongoDB{
			database:        db,
			collectionMatch: collectionMatch,
			pipe:            p,
			path:            path,
			conf:            conf,
			writer:          newWriter(),
			reader:          newReader(db),
			doneChannel:     make(chan struct{}),
		}

		if conf.Bulk {
			m.writer = newBulker(m.doneChannel, &m.wg)
		}

		if conf.Tail {
			m.reader = newTailer(db)
		}

		clientOptions := []ClientOptionFunc{
			WithURI(conf.URI),
			WithTimeout(conf.Timeout),
			WithSSL(conf.SSL),
			WithCACerts(conf.CACerts),
			WithFsync(conf.FSync),
			WithTail(conf.Tail),
		}

		if conf.Wc > 0 {
			clientOptions = append(clientOptions, WithWriteConcern(conf.Wc))
		}

		m.client, err = NewClient(clientOptions...)
		if err != nil {
			return m, err
		}

		m.database, m.collectionMatch, err = extra.CompileNamespace()
		if err != nil {
			return m, err
		}

		return m, nil
	})
}

// Description for mongodb adaptor
func (m *MongoDB) Description() string {
	return description
}

// SampleConfig for mongodb adaptor
func (m *MongoDB) SampleConfig() string {
	return sampleConfig
}

// Connect tests the mongodb connection and initializes the mongo session
func (m *MongoDB) Connect() error {
	_, err := m.client.Connect()
	return err
}

// Start the adaptor as a source
func (m *MongoDB) Start() (err error) {
	log.With("path", m.path).Infoln("adaptor Starting...")
	defer func() {
		m.pipe.Stop()
	}()

	s, err := m.client.Connect()
	if err != nil {
		return err
	}
	readFunc := m.reader.Read(m.collectionFilter)
	msgChan, err := readFunc(s, m.doneChannel)
	if err != nil {
		return err
	}
	for msg := range msgChan {
		m.pipe.Send(msg)
	}

	log.With("path", m.path).Infoln("adaptor Start finished...")
	return nil
}

// Listen starts the pipe's listener
func (m *MongoDB) Listen() (err error) {
	log.With("path", m.path).Infoln("adaptor Listening...")
	defer func() {
		log.With("path", m.path).Infoln("adaptor Listen closing...")
		m.pipe.Stop()
	}()

	return m.pipe.Listen(m.writeMessage, m.collectionMatch)
}

// Stop the adaptor
func (m *MongoDB) Stop() error {
	log.With("path", m.path).Infoln("adaptor Stopping...")
	m.pipe.Stop()

	close(m.doneChannel)
	m.wg.Wait()

	log.With("path", m.path).Infoln("adaptor Stopped")
	return nil
}

// writeMessage writes one message to the destination mongo, or sends an error down the pipe
func (m *MongoDB) writeMessage(msg message.Msg) (message.Msg, error) {
	_, msgColl, _ := message.SplitNamespace(msg)
	err := client.Write(m.client, m.writer, message.From(msg.OP(), m.computeNamespace(msgColl), msg.Data()))

	if err != nil {
		m.pipe.Err <- adaptor.NewError(adaptor.ERROR, m.path, fmt.Sprintf("write message error (%s)", err), msg.Data)
	}
	return msg, err
}

func (m *MongoDB) collectionFilter(collection string) bool {
	if strings.HasPrefix(collection, "system.") {
		return false
	}
	return m.collectionMatch.MatchString(collection)
}

func (m *MongoDB) computeNamespace(collection string) string {
	return fmt.Sprintf("%s.%s", m.database, collection)
}

// Config provides configuration options for a mongodb adaptor
// the notable difference between this and dbConfig is the presence of the Tail option
type Config struct {
	URI       string   `json:"uri" doc:"the uri to connect to, in the form mongodb://user:password@host.com:27017/auth_database"`
	Namespace string   `json:"namespace" doc:"mongo namespace to read/write"`
	SSL       bool     `json:"ssl" doc:"ssl options for connection"`
	CACerts   []string `json:"cacerts" doc:"array of root CAs to use in order to verify the server certificates"`
	Timeout   string   `json:"timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	Tail      bool     `json:"tail" doc:"if tail is true, then the mongodb source will tail the oplog after copying the namespace"`
	Wc        int      `json:"wc" doc:"The write concern to use for writes, Int, indicating the minimum number of servers to write to before returning success/failure"`
	FSync     bool     `json:"fsync" doc:"When writing, should we flush to disk before returning success"`
	Bulk      bool     `json:"bulk" doc:"use a buffer to bulk insert documents"`
}
