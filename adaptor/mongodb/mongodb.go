package mongodb

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
)

const (
	description = "a mongodb adaptor that functions as both a source and a sink"

	sampleConfig = `{
  "uri": "${MONGODB_URI}"
  // "timeout": "30s",
  // "tail": false,
  // "ssl": false,
  // "cacerts": ["/path/to/cert.pem"],
  // "wc": 1,
  // "fsync": false,
  // "bulk": false,
  // "collection_filters": "{}",
  // "read_preference": "Primary"
}`
)

var (
	_ adaptor.Adaptor = &MongoDB{}

	// ErrCollectionFilter is returned when an error occurs attempting to Unmarshal the string.
	ErrCollectionFilter = errors.New("malformed collection_filters")
)

// MongoDB is an adaptor to read / write to mongodb.
// it works as a source by copying files, and then optionally tailing the oplog
type MongoDB struct {
	adaptor.BaseConfig
	SSL               bool     `json:"ssl"`
	CACerts           []string `json:"cacerts"`
	Tail              bool     `json:"tail"`
	Wc                int      `json:"wc"`
	FSync             bool     `json:"fsync"`
	Bulk              bool     `json:"bulk"`
	CollectionFilters string   `json:"collection_filters"`
	ReadPreference    string   `json:"read_preference"`
}

func init() {
	adaptor.Add(
		"mongodb",
		func() adaptor.Adaptor {
			return &MongoDB{}
		},
	)
}

func (m *MongoDB) Client() (client.Client, error) {
	return NewClient(WithURI(m.URI),
		WithTimeout(m.Timeout),
		WithSSL(m.SSL),
		WithCACerts(m.CACerts),
		WithFsync(m.FSync),
		WithTail(m.Tail),
		WithWriteConcern(m.Wc),
		WithReadPreference(m.ReadPreference))
}

func (m *MongoDB) Reader() (client.Reader, error) {
	var f map[string]CollectionFilter
	if m.CollectionFilters != "" {
		if jerr := json.Unmarshal([]byte(m.CollectionFilters), &f); jerr != nil {
			return nil, ErrCollectionFilter
		}
	}
	return newReader(m.Tail, f), nil
}

func (m *MongoDB) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	if m.Bulk {
		return newBulker(done, wg), nil
	}
	return newWriter(), nil
}

// Description for mongodb adaptor
func (m *MongoDB) Description() string {
	return description
}

// SampleConfig for mongodb adaptor
func (m *MongoDB) SampleConfig() string {
	return sampleConfig
}
