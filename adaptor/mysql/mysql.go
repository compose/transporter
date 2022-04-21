package mysql

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"

	//_ "github.com/go-sql-driver/mysql" // import mysql driver
	_ "github.com/go-mysql-org/go-mysql/driver" // import alternative mysql driver
)

const (
	description = "a mysql adaptor that functions as both a source and a sink"

	sampleConfig = `{
  "uri": "${MYSQL_URI}",
  // "tail": false,
  // "cacert": "/path/to/cert.pem",
  // "servername": "${MYSQL_DOMAIN}",
}`
)

var (
	_ adaptor.Adaptor = &mysql{}
)

// MySQL is an adaptor to read / write to mysql.
// it works as a source by copying files, and then optionally tailing the binlog
type mysql struct {
	adaptor.BaseConfig
	Tail       bool   `json:"tail" doc:"if tail is true, then the mysql source will tail the binlog after copying the namespace"`
	CACert     string `json:"cacert" doc:"path to CA cert"`
	ServerName string `json:"servername" doc:"if a separate servername is needed to verify the certificate against. Requires cacert"`
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
	return NewClient(WithURI(m.URI),
		WithCustomTLS(m.URI, m.CACert, m.ServerName))
}

func (m *mysql) Reader() (client.Reader, error) {
	if m.Tail {
		return newTailer(m.URI), nil
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
