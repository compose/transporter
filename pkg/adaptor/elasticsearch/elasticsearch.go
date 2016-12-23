package elasticsearch

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	"github.com/hashicorp/go-version"
)

// Elasticsearch is an adaptor to connect a pipeline to
// an elasticsearch cluster.
type Elasticsearch struct {
	// pull these in from the node
	uri *url.URL

	index     string
	typeMatch *regexp.Regexp

	pipe *pipe.Pipe
	path string

	// indexer *elastigo.BulkIndexer
	running bool
}

// Description for the Elasticsearcb adaptor
func (e *Elasticsearch) Description() string {
	return "an elasticsearch sink adaptor"
}

const sampleConfig = `
- es:
		type: elasticsearch
    uri: https://username:password@hostname:port/thisgetsignored
`

// SampleConfig for elasticsearch adaptor
func (e *Elasticsearch) SampleConfig() string {
	return sampleConfig
}

func init() {
	adaptor.Add("elasticsearch", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf adaptor.DbConfig
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("bad config (%s)", err.Error()), nil)
		}

		u, err := url.Parse(conf.URI)
		if err != nil {
			return nil, err
		}

		e := &Elasticsearch{
			uri:  u,
			pipe: p,
			path: path,
		}

		e.index, e.typeMatch, err = extra.CompileNamespace()
		if err != nil {
			return e, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("can't split namespace into _index and typeMatch (%s)", err.Error()), nil)
		}

		return e, nil
	})
}

// Start the adaptor as a source (not implemented)
func (e *Elasticsearch) Start() error {
	return fmt.Errorf("elasticsearch can't function as a source")
}

// Listen starts the listener
func (e *Elasticsearch) Listen() error {
	e.setupClient()
	// e.indexer.Start()
	e.running = true

	// go func(cherr chan *elastigo.ErrorBuffer) {
	// 	for err := range e.indexer.ErrorChannel {
	// 		e.pipe.Err <- adaptor.NewError(adaptor.CRITICAL, e.path, fmt.Sprintf("elasticsearch error (%s)", err.Err), nil)
	// 	}
	// }(e.indexer.ErrorChannel)

	defer func() {
		if e.running {
			e.running = false
			e.pipe.Stop()
			// e.indexer.Stop()
		}
	}()

	return e.pipe.Listen(e.applyOp, e.typeMatch)
}

// Stop the adaptor
func (e *Elasticsearch) Stop() error {
	if e.running {
		e.running = false
		e.pipe.Stop()
		// e.indexer.Stop()
	}
	return nil
}

func (e *Elasticsearch) applyOp(msg message.Msg) (message.Msg, error) {
	// m, err := message.Exec(message.MustUseAdaptor("elasticsearch").(elasticsearch.Adaptor).UseIndexer(e.indexer).UseIndex(e.index), msg)
	// if err != nil {
	// 	e.pipe.Err <- adaptor.NewError(adaptor.ERROR, e.path, fmt.Sprintf("elasticsearch error (%s)", err), msg.Data)
	// }
	// return m, err
	return nil, nil
}

func (e *Elasticsearch) setupClient() {
	stringVersion, _ := e.determineVersion()
	v, _ := version.NewVersion(stringVersion)
	v1client, _ := version.NewConstraint(">= 1.4, < 2.0")
	v2client, _ := version.NewConstraint(">= 2.0, < 5.0")
	v5client, _ := version.NewConstraint(">= 5.0")
	if v1client.Check(v) {
		fmt.Println("setting up v1 client")
	} else if v2client.Check(v) {
		fmt.Println("setting up v2 client")
	} else if v5client.Check(v) {
		fmt.Println("setting up v5 client")
	} else {
		fmt.Printf("unable to setup client for version: %s\n", stringVersion)
	}
	// set up the client, we need host(s), port, username, password, and scheme
	// client := elastigo.NewConn()
	//
	// if e.uri.User != nil {
	// 	client.Username = e.uri.User.Username()
	// 	if password, set := e.uri.User.Password(); set {
	// 		client.Password = password
	// 	}
	// }
	//
	// // we might have a port in the host bit
	// hostBits := strings.Split(e.uri.Host, ":")
	// if len(hostBits) > 1 {
	// 	client.SetPort(hostBits[1])
	// }
	//
	// client.SetHosts(strings.Split(hostBits[0], ","))
	// client.Protocol = e.uri.Scheme
	//
	// e.indexer = client.NewBulkIndexerErrors(10, 60)
}

func (e *Elasticsearch) determineVersion() (string, error) {
	resp, err := http.DefaultClient.Get(e.uri.String())
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	var bareResponse struct {
		Name    string `json:"name"`
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	err = json.Unmarshal(body, &bareResponse)
	if err != nil {
		return "", fmt.Errorf("unable to determine version from response, %s\n", string(body))
	}
	return bareResponse.Version.Number, nil
}
