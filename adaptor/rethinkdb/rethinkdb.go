package rethinkdb

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
)

const (
	sampleConfig = `{
   "uri": "${RETHINKDB_URI}"
  // "timeout": "30s",
  // "tail": false,
  // "ssl": false,
  // "cacerts": ["/path/to/cert.pem"]
}`

	description = "a rethinkdb adaptor that functions as both a source and a sink"
)

var (
	_ adaptor.Adaptor = &RethinkDB{}
)

// RethinkDB is an adaptor that writes metrics to rethinkdb (http://rethinkdb.com/)
// An open-source distributed database
type RethinkDB struct {
	adaptor.BaseConfig
	Tail    bool     `json:"tail"`
	SSL     bool     `json:"ssl"`
	CACerts []string `json:"cacerts"`
}

func init() {
	adaptor.Add(
		"rethinkdb",
		func() adaptor.Adaptor {
			return &RethinkDB{}
		},
	)
}

func (r *RethinkDB) Client() (client.Client, error) {
	// TODO: pull db from the URI
	db, _, _ := adaptor.CompileNamespace(r.Namespace)
	return NewClient(
		WithURI(r.URI),
		WithDatabase(db),
		WithSessionTimeout(r.Timeout),
		WithSSL(r.SSL),
		WithCACerts(r.CACerts),
	)
}

func (r *RethinkDB) Reader() (client.Reader, error) {
	// TODO: pull db from the URI
	db, _, err := adaptor.CompileNamespace(r.Namespace)
	return newReader(db, r.Tail), err
}

func (r *RethinkDB) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	// TODO: pull db from the URI
	db, _, err := adaptor.CompileNamespace(r.Namespace)
	return newWriter(db, done, wg), err
}

// Description for rethinkdb adaptor
func (r *RethinkDB) Description() string {
	return description
}

// SampleConfig for rethinkdb adaptor
func (r *RethinkDB) SampleConfig() string {
	return sampleConfig
}
