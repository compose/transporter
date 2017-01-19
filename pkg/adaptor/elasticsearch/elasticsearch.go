package elasticsearch

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/adaptor/elasticsearch/clients"
	// used to call init function for each client to register itself
	_ "github.com/compose/transporter/pkg/adaptor/elasticsearch/clients/all"
	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	"github.com/hashicorp/go-version"
)

// Elasticsearch is an adaptor to connect a pipeline to
// an elasticsearch cluster.
type Elasticsearch struct {
	client client.Writer

	index     string
	typeMatch *regexp.Regexp

	pipe *pipe.Pipe
	path string

	doneChannel chan struct{}
	wg          sync.WaitGroup
}

// Description for the Elasticsearcb adaptor
func (e *Elasticsearch) Description() string {
	return "an elasticsearch sink adaptor"
}

const sampleConfig = `
- es:
		type: elasticsearch
    uri: https://username:password@hostname:port
`

// SampleConfig for elasticsearch adaptor
func (e *Elasticsearch) SampleConfig() string {
	return sampleConfig
}

func init() {
	adaptor.Add("elasticsearch", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf Config
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("bad config (%s)", err.Error()), nil)
		}
		log.With("path", path).Debugf("adaptor config: %+v", conf)

		e := &Elasticsearch{
			pipe:        p,
			path:        path,
			doneChannel: make(chan struct{}),
		}

		e.index, e.typeMatch, err = extra.CompileNamespace()
		if err != nil {
			return e, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("can't split namespace into _index and typeMatch (%s)", err.Error()), nil)
		}

		if err := e.setupClient(conf); err != nil {
			return nil, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("unable to setup client (%s)", err), nil)
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
	log.With("path", e.path).Infoln("adaptor Listening...")
	defer func() {
		log.With("path", e.path).Infoln("adaptor Listen closing...")
		e.pipe.Stop()
	}()

	return e.pipe.Listen(e.applyOp, e.typeMatch)
}

// Stop the adaptor
func (e *Elasticsearch) Stop() error {
	log.With("path", e.path).Infoln("adaptor Stopping...")
	e.pipe.Stop()

	close(e.doneChannel)
	e.wg.Wait()

	log.With("path", e.path).Infoln("adaptor Stopped")
	return nil
}

func (e *Elasticsearch) applyOp(msg message.Msg) (message.Msg, error) {
	_, msgColl, _ := message.SplitNamespace(msg)
	err := e.client.Write(From(msg.OP(), e.computeNamespace(msgColl), msg.Data()))(nil)

	if err != nil {
		e.pipe.Err <- adaptor.NewError(adaptor.ERROR, e.path, fmt.Sprintf("write message error (%s)", err), msg.Data)
	}
	return msg, err
}

func (e *Elasticsearch) computeNamespace(Type string) string {
	return fmt.Sprintf("%s.%s", e.index, Type)
}

func (e *Elasticsearch) setupClient(conf Config) error {
	uri, err := url.Parse(conf.URI)
	if err != nil {
		return err
	}
	hostsAndPorts := strings.Split(uri.Host, ",")
	stringVersion, err := determineVersion(fmt.Sprintf("%s://%s", uri.Scheme, hostsAndPorts[0]))
	if err != nil {
		return err
	}

	v, err := version.NewVersion(stringVersion)
	if err != nil {
		return err
	}

	httpClient := http.DefaultClient
	if conf.Timeout != "" {
		t, err := time.ParseDuration(conf.Timeout)
		if err != nil {
			return err
		}
		httpClient = &http.Client{
			Timeout: t,
		}
	}

	for _, vc := range clients.Clients {
		if vc.Constraint.Check(v) {
			urls := make([]string, len(hostsAndPorts))
			for i, hAndP := range hostsAndPorts {
				urls[i] = fmt.Sprintf("%s://%s", uri.Scheme, hAndP)
			}
			opts := &clients.ClientOptions{
				URLs:       urls,
				UserInfo:   uri.User,
				HTTPClient: httpClient,
				Path:       e.path,
			}
			versionedClient, _ := vc.Creator(e.doneChannel, &e.wg, opts)
			e.client = versionedClient
			return nil
		}
	}

	return fmt.Errorf("no client registered for version %s\n", stringVersion)
}

func determineVersion(uri string) (string, error) {
	resp, err := http.DefaultClient.Get(uri)
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var r struct {
		Name    string `json:"name"`
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	err = json.Unmarshal(body, &r)
	if err != nil {
		return "", fmt.Errorf("unable to determine version from response, %s\n", string(body))
	}
	return r.Version.Number, nil
}

// Config provides configuration options for an elasticsearch adaptor
// the notable difference between this and dbConfig is the presence of the Timeout option
type Config struct {
	URI       string `json:"uri" doc:"the uri to connect to, in the form mongodb://user:password@host.com:27017/auth_database"`
	Namespace string `json:"namespace" doc:"mongo namespace to read/write"`
	Timeout   string `json:"timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
}
