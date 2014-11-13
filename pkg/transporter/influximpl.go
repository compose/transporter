package transporter

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
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
	pipe pipe.Pipe

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

func (i *InfluxImpl) Start(pipe pipe.Pipe) (err error) {
	i.pipe = pipe
	i.influxClient, err = i.setupClient()
	if err != nil {
		i.pipe.Err <- err
		return err
	}

	return i.pipe.Listen(i.applyOp)
}

func (i *InfluxImpl) String() string {
	return fmt.Sprintf("%-20s %-15s %-30s %s", n.Name, "influx", strings.Join([]string{i.database, i.series_name}, "."), i.uri.String())
}

// func (i *InfluxImpl) Config() ConfigNode {
// 	return i.config
// }

func (i *InfluxImpl) Stop() error {
	i.pipe.Stop()
	return nil
}

func (i *InfluxImpl) applyOp(msg *message.Msg) (err error) {
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
