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
  // "replication_slot": "slot",
  // "case_sensitive_identifiers": false
}`
)

var (
	_ adaptor.Adaptor = &postgres{}
)

// Postgres is an adaptor to read / write to postgres.
// it works as a source by copying files, and then optionally tailing the oplog
type postgres struct {
	adaptor.BaseConfig
	Debug                    bool   `json:"debug" doc:"display debug information"`
	Tail                     bool   `json:"tail" doc:"if tail is true, then the postgres source will tail the oplog after copying the namespace"`
	ReplicationSlot          string `json:"replication_slot" doc:"required if tail is true; sets the replication slot to use for logical decoding"`
	CaseSensitiveIdentifiers bool   `json:"case_sensitive_identifiers" doc:"if true, will add quotation marks to table identifiers"`
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
	var newTableFunc func() table

	if p.CaseSensitiveIdentifiers {
		newTableFunc = newCaseSensitiveTable
	} else {
		newTableFunc = newCaseInsensitiveTable
	}

	reader := newReader(newTableFunc)
	if p.Tail {
		return newTailer(reader, p.ReplicationSlot), nil
	}

	return reader, nil
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
