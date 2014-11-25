package impl

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	"github.com/influxdb/influxdb/client"
)

type Influxdb struct {
	// pull these in from the node
	uri *url.URL

	// save time by setting these once
	database    string
	series_name string

	//
	pipe *pipe.Pipe

	// influx connection and options
	influxClient *client.Client
}

func NewInfluxdb(p *pipe.Pipe, extra ExtraConfig) (*Influxdb, error) {
	var (
		conf InfluxdbConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	u, err := url.Parse(conf.Uri)
	if err != nil {
		return nil, err
	}

	i := &Influxdb{
		uri:  u,
		pipe: p,
	}

	i.database, i.series_name, err = i.splitNamespace(conf.Namespace)
	if err != nil {
		return i, err
	}

	return i, nil
}

func (i *Influxdb) Listen() (err error) {
	i.influxClient, err = i.setupClient()
	if err != nil {
		i.pipe.Err <- err
		return err
	}

	return i.pipe.Listen(i.applyOp)
}

func (i *Influxdb) Stop() error {
	i.pipe.Stop()
	return nil
}

func (i *Influxdb) applyOp(msg *message.Msg) (*message.Msg, error) {
	switch msg.Op {
	case message.Insert:
		docSize := len(msg.Document())
		columns := make([]string, 0, docSize)
		points := make([][]interface{}, 1)
		points[0] = make([]interface{}, 0, docSize)
		for k := range msg.Document() {
			columns = append(columns, k)
			points[0] = append(points[0], msg.Document()[k])
		}
		series := &client.Series{
			Name:    i.series_name,
			Columns: columns,
			Points:  points,
		}

		return msg, i.influxClient.WriteSeries([]*client.Series{series})
	}
	return msg, nil
}

func (i *Influxdb) setupClient() (influxClient *client.Client, err error) {
	// set up the clientConfig, we need host:port, username, password, and database name
	clientConfig := &client.ClientConfig{
		Database: i.database,
	}

	if i.uri.User != nil {
		clientConfig.Username = i.uri.User.Username()
		if password, set := i.uri.User.Password(); set {
			clientConfig.Password = password
		}
	}
	clientConfig.Host = i.uri.Host

	return client.NewClient(clientConfig)
}

/*
 * split a influx namespace into a database and a series name
 */
func (i *Influxdb) splitNamespace(namespace string) (string, string, error) {
	fields := strings.SplitN(namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed influx namespace.")
	}
	return fields[0], fields[1], nil
}

// InfluxdbConfig options
type InfluxdbConfig struct {
	Uri       string `json:"uri"`       // the database uri
	Namespace string `json:"namespace"` // namespace
	Debug     bool   `json:"debug"`     // debug mode
}
