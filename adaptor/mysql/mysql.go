package mysql

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"

	_ "github.com/go-sql-driver/mysql" // import mysql driver
)

const (
	description = "a mysql adaptor that functions as both a source and a sink"

	sampleConfig = `{
  "uri": "${MYSQL_URI}"
  // "debug": false,
  // "tail": false,
}`
)

var (
	_ adaptor.Adaptor = &mysql{}
)

// MySQL is an adaptor to read / write to mysql.
// it works as a source by copying files, and then optionally tailing the binlog
type mysql struct {
	adaptor.BaseConfig
	Debug           bool   `json:"debug" doc:"display debug information"`
	Tail            bool   `json:"tail" doc:"if tail is true, then the mysql source will tail the binlog after copying the namespace"`
}

func init() {
	adaptor.Add(
		"mysql",
		func() adaptor.Adaptor {
			return &mysql{}
		},
	)
}

func (m *mysql) Client() (client.Client, error) {
	return NewClient(WithURI(m.URI))
}

func (m *mysql) Reader() (client.Reader, error) {
	if m.Tail {
		// Not implemented yet
	}
	return newReader(), nil
}

func (m *mysql) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return newWriter(), nil
}

// Description for mysql adaptor
func (m *mysql) Description() string {
	return description
}

// SampleConfig for mysql adaptor
func (m *mysql) SampleConfig() string {
	return sampleConfig
}
