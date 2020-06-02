package postgres

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"

	_ "github.com/lib/pq" // import pq driver
)

const (
	description = "a postgres adaptor that functions as both a source and a sink"

	sampleConfig = `{
  "uri": "${POSTGRESQL_URI}"
  // "debug": false,
  // "tail": false,
  // "replication_slot": "slot"
}`
)

var (
	_ adaptor.Adaptor = &postgres{}
)

// Postgres is an adaptor to read / write to postgres.
// it works as a source by copying files, and then optionally tailing the oplog
type postgres struct {
	adaptor.BaseConfig
	Debug           bool   `json:"debug" doc:"display debug information"`
	Tail            bool   `json:"tail" doc:"if tail is true, then the postgres source will tail the oplog after copying the namespace"`
	ReplicationSlot string `json:"replication_slot" doc:"required if tail is true; sets the replication slot to use for logical decoding"`
}

func init() {
	adaptor.Add(
		"postgres",
		func() adaptor.Adaptor {
			return &postgres{}
		},
	)
}

func (p *postgres) Client() (client.Client, error) {
	return NewClient(WithURI(p.URI))
}

func (p *postgres) Reader() (client.Reader, error) {
	if p.Tail {
		return newTailer(p.ReplicationSlot), nil
	}
	return newReader(), nil
}

func (p *postgres) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return newWriter(), nil
}

// Description for postgres adaptor
func (p *postgres) Description() string {
	return description
}

// SampleConfig for postgres adaptor
func (p *postgres) SampleConfig() string {
	return sampleConfig
}
