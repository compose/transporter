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
	_ adaptor.Adaptor = &rethinkDB{}
)

// RethinkDB is an adaptor that writes metrics to rethinkdb (http://rethinkdb.com/)
// An open-source distributed database
type rethinkDB struct {
	adaptor.BaseConfig
	Tail    bool     `json:"tail"`
	SSL     bool     `json:"ssl"`
	CACerts []string `json:"cacerts"`
}

func init() {
	adaptor.Add(
		"rethinkdb",
		func() adaptor.Adaptor {
			return &rethinkDB{}
		},
	)
}

func (r *rethinkDB) Client() (client.Client, error) {
	// TODO: pull db from the URI
	return NewClient(
		WithURI(r.URI),
		WithSessionTimeout(r.Timeout),
		WithSSL(r.SSL),
		WithCACerts(r.CACerts),
	)
}

func (r *rethinkDB) Reader() (client.Reader, error) {
	return newReader(r.Tail), nil
}

func (r *rethinkDB) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return newWriter(done, wg), nil
}

// Description for rethinkdb adaptor
func (r *rethinkDB) Description() string {
	return description
}

// SampleConfig for rethinkdb adaptor
func (r *rethinkDB) SampleConfig() string {
	return sampleConfig
}
