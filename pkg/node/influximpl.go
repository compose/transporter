package node

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/compose/transporter/pkg/message"
	"github.com/influxdb/influxdb/client"
)

type InfluxImpl struct {
	// pull these in from the node
	uri *url.URL

	// save time by setting these once
	database    string
	series_name string

	config ConfigNode

	//
	pipe Pipe

	// influx connection and options
	influxClient *client.Client
}

func NewInfluxImpl(c ConfigNode) (*InfluxImpl, error) {
	u, err := url.Parse(c.Uri)
	if err != nil {
		return nil, err
	}

	i := &InfluxImpl{
		config: c,
		uri:    u,
	}

	return i, nil
}

func (i *InfluxImpl) Start(pipe Pipe) (err error) {
	i.pipe = pipe
	i.influxClient, err = i.setupClient()
	if err != nil {
		i.pipe.Err <- err
		return err
	}

	return i.pipe.Listen(i.applyOp)
}

func (i *InfluxImpl) applyOp(msg *message.Msg) (err error) {
	series := &client.Series{
		Name:    i.series_name,
		Columns: []string{"value"},
		Points: [][]interface{}{
			{1.0},
		},
	}

	return i.influxClient.WriteSeries([]*client.Series{series})
}

func (i *InfluxImpl) setupClient() (influxClient *client.Client, err error) {
	// split the namespace into the database and series name
	i.database, i.series_name, err = i.splitNamespace()

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
func (i *InfluxImpl) splitNamespace() (string, string, error) {
	fields := strings.SplitN(i.config.Namespace, ".", 2)

	if len(fields) != 2 {
		return "", "", fmt.Errorf("malformed influx namespace.")
	}
	return fields[0], fields[1], nil
}
