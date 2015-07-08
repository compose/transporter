package adaptor

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/tsdb"
)

const (
	INFLUX_BUFFER_LEN int = 1e3
)

// Influxdb is an adaptor that writes metrics to influxdb (https://influxdb.com/)
// a high performant time series database
type Influxdb struct {
	// pull these in from the node
	uri *url.URL

	// save time by setting these once
	database        string
	retentionPolicy string
	tags            map[string]string

	//
	pipe *pipe.Pipe
	path string

	// influx connection and options
	influxClient *client.Client

	// a buffer to hold documents
	buffLock         sync.Mutex
	opsBuffer        []client.Point
	bulkWriteChannel chan client.Point
	bulkQuitChannel  chan chan bool
}

// NewInfluxdb creates an Influxdb adaptor
func NewInfluxdb(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
	var (
		conf InfluxdbConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	if conf.URI == "" || conf.Namespace == "" {
		return nil, fmt.Errorf("both uri and namespace required, but missing one")
	}

	u, err := url.Parse(conf.URI)
	if err != nil {
		return nil, err
	}

	i := &Influxdb{
		uri:              u,
		database:         conf.Namespace,
		retentionPolicy:  conf.RetentionPolicy,
		tags:             conf.Tags,
		pipe:             p,
		path:             path,
		opsBuffer:        make([]client.Point, 0, INFLUX_BUFFER_LEN),
		bulkWriteChannel: make(chan client.Point),
		bulkQuitChannel:  make(chan chan bool),
	}

	if i.retentionPolicy == "" {
		i.retentionPolicy = "default"
	}

	i.influxClient, err = i.setupClient()
	if err != nil {
		return i, err
	}

	_, _, err = i.influxClient.Ping()
	if err != nil {
		return i, err
	}

	return i, nil
}

// Start the adaptor as a source (not implemented)
func (i *Influxdb) Start() error {
	return fmt.Errorf("influxdb can't function as a source")
}

// Listen starts the listener
func (i *Influxdb) Listen() (err error) {
	defer func() {
		i.pipe.Stop()
	}()

	go i.bulkWriter()
	return i.pipe.Listen(i.applyOp)
}

// Stop the adaptor
func (i *Influxdb) Stop() error {
	i.pipe.Stop()

	q := make(chan bool)
	i.bulkQuitChannel <- q
	<-q

	return nil
}

func (i *Influxdb) applyOp(msg *message.Msg) (*message.Msg, error) {
	switch msg.Op {
	case message.Insert:
		if line, isString := msg.Data.(string); isString {
			// parse line protocol
			points, err := tsdb.ParsePoints([]byte(line))
			if err != nil {
				i.pipe.Err <- NewError(ERROR, i.path, "influxdb error (unable to parse string as line protocol)", msg.Data)
				return msg, nil
			}
			for _, point := range points {
				pt := client.Point{
					Raw: point.String(),
				}
				i.bulkWriteChannel <- pt
			}
		} else if !msg.IsMap() {
			i.pipe.Err <- NewError(ERROR, i.path, "influxdb error (document must be a json document)", msg.Data)
			return msg, nil
		} else {
			// pull data from json doc
			doc := msg.Map()
			bytes, err := json.Marshal(doc)
			if err != nil {
				i.pipe.Err <- NewError(ERROR, i.path, "influxdb error (unable to marshal doc)", msg.Data)
				return msg, nil
			}

			var pt client.Point
			if err := pt.UnmarshalJSON(bytes); err != nil {
				i.pipe.Err <- NewError(ERROR, i.path, "influxdb error (unable to unmarshal to point)", msg.Data)
				return msg, nil
			}
			i.bulkWriteChannel <- pt
		}
	}
	return msg, nil
}

func (i *Influxdb) bulkWriter() {
	for {
		select {
		case doc := <-i.bulkWriteChannel:
			if len(i.opsBuffer) == INFLUX_BUFFER_LEN {
				i.writeBuffer() // send it off to be inserted
			}

			i.buffLock.Lock()
			i.opsBuffer = append(i.opsBuffer, doc)
			i.buffLock.Unlock()
		case <-time.After(2 * time.Second):
			i.writeBuffer()
		case q := <-i.bulkQuitChannel:
			i.writeBuffer()
			q <- true
		}
	}
}

func (i *Influxdb) writeBuffer() {
	i.buffLock.Lock()
	defer i.buffLock.Unlock()
	if len(i.opsBuffer) == 0 {
		return
	}

	bps := client.BatchPoints{
		Points:          i.opsBuffer,
		Database:        i.database,
		RetentionPolicy: i.retentionPolicy,
	}

	resp, err := i.influxClient.Write(bps)
	if err != nil {
		i.pipe.Err <- NewError(ERROR, i.path, fmt.Sprintf("influxdb batch error (%s)", err.Error()), nil)
	}
	for _, result := range resp.Results {
		if result.Err != nil {
			i.pipe.Err <- NewError(ERROR, i.path, fmt.Sprintf("influxdb write error (%s)", result.Err.Error()), result.Series[0])
		}
	}

	i.opsBuffer = make([]client.Point, 0, INFLUX_BUFFER_LEN)
}

func (i *Influxdb) setupClient() (influxClient *client.Client, err error) {
	u, _ := url.Parse(fmt.Sprintf("%s//%s", i.uri.Scheme, i.uri.Host))
	conf := &client.Config{
		URL:       *u,
		UserAgent: "Transporter",
	}

	if i.uri.User != nil {
		conf.Username = i.uri.User.Username()
		if password, set := i.uri.User.Password(); set {
			conf.Password = password
		}
	}

	return client.NewClient(*conf)
}

// InfluxdbConfig provides configuration options for an influxdb adaptor
// the notable difference between this and dbConfig is the presence of the Tags and RetentionPolicy option
type InfluxdbConfig struct {
	URI             string            `json:"uri" doc:"the uri to connect to, in the form https://user:password@host.com:27017/database"`
	Namespace       string            `json:"namespace" doc:"influxdb namespace to read/write (a.k.a. database)"`
	Debug           bool              `json:"debug" doc:"display debug information"`
	Tags            map[string]string `json:"tags" doc:"default tags to include for each measurement"`
	RetentionPolicy string            `json:"retention_policy" doc:"specify a custom retention policy for the database"`
}
