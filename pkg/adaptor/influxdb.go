package adaptor

/*
 * Influx is awesome, but unfortunately the pre-0.9 changes
 * they are going through seem to being breaking the build
 * I was hoping that we would be able to bring the client
 * in via gopkg.in, but that doesn't seem to work either.
 * commenting out this adaptor until the state changes, or until
 * we get some proper vendoring of these packages
 */

// import (
// 	"fmt"
// 	"net/url"

// 	"github.com/compose/transporter/pkg/message"
// 	"github.com/compose/transporter/pkg/pipe"
// 	"github.com/influxdb/influxdb/client"
// )

// // Influxdb is an adaptor that writes metrics to influxdb (http://influxdb.com/)
// // a high performant time series database
// type Influxdb struct {
// 	// pull these in from the node
// 	uri *url.URL

// 	// save time by setting these once
// 	database   string
// 	seriesName string

// 	//
// 	pipe *pipe.Pipe
// 	path string

// 	// influx connection and options
// 	influxClient *client.Client
// }

// // NewInfluxdb creates an Influxdb adaptor
// func NewInfluxdb(p *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
// 	var (
// 		conf dbConfig
// 		err  error
// 	)
// 	if err = extra.Construct(&conf); err != nil {
// 		return nil, err
// 	}

// 	u, err := url.Parse(conf.URI)
// 	if err != nil {
// 		return nil, err
// 	}

// 	i := &Influxdb{
// 		uri:  u,
// 		pipe: p,
// 		path: path,
// 	}

// 	i.database, i.seriesName, err = extra.splitNamespace()
// 	if err != nil {
// 		return i, err
// 	}

// 	return i, nil
// }

// // Start the adaptor as a source (not implemented)
// func (i *Influxdb) Start() error {
// 	return fmt.Errorf("influxdb can't function as a source")
// }

// // Listen starts the listener
// func (i *Influxdb) Listen() (err error) {
// 	i.influxClient, err = i.setupClient()
// 	if err != nil {
// 		i.pipe.Err <- err
// 		return err
// 	}

// 	return i.pipe.Listen(i.applyOp)
// }

// // Stop the adaptor
// func (i *Influxdb) Stop() error {
// 	i.pipe.Stop()
// 	return nil
// }

// func (i *Influxdb) applyOp(msg *message.Msg) (*message.Msg, error) {
// 	switch msg.Op {
// 	case message.Insert:
// 		if !msg.IsMap() {
// 			i.pipe.Err <- NewError(ERROR, i.path, "influxdb error (document must be a json document)", msg.Data)
// 			return msg, nil
// 		}
// 		doc := msg.Map()

// 		sz := len(doc)
// 		columns := make([]string, 0, sz)
// 		points := make([][]interface{}, 1)
// 		points[0] = make([]interface{}, 0, sz)
// 		for k, v := range doc {
// 			columns = append(columns, k)
// 			points[0] = append(points[0], v)
// 		}
// 		series := &client.Series{
// 			Name:    i.seriesName,
// 			Columns: columns,
// 			Points:  points,
// 		}

// 		return msg, i.influxClient.WriteSeries([]*client.Series{series})
// 	}
// 	return msg, nil
// }

// func (i *Influxdb) setupClient() (influxClient *client.Client, err error) {
// 	// set up the clientConfig, we need host:port, username, password, and database name
// 	clientConfig := &client.ClientConfig{
// 		Database: i.database,
// 	}

// 	if i.uri.User != nil {
// 		clientConfig.Username = i.uri.User.Username()
// 		if password, set := i.uri.User.Password(); set {
// 			clientConfig.Password = password
// 		}
// 	}
// 	clientConfig.Host = i.uri.Host

// 	return client.NewClient(clientConfig)
// }
