package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/adaptor/elasticsearch/clients"
	// used to call init function for each client to register itself
	_ "github.com/compose/transporter/adaptor/elasticsearch/clients/all"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/pipe"
	version "github.com/hashicorp/go-version"
)

const (
	description  = "an elasticsearch sink adaptor"
	sampleConfig = `    type: elasticsearch
    uri: https://username:password@hostname:port
    # timeout: 10s # defaults to 30s
    # aws_access_key: XXX # used for signing requests to AWS Elasticsearch service
    # aws_access_secret: XXX # used for signing requests to AWS Elasticsearch service`
)

var (
	_ adaptor.Adaptor = &Elasticsearch{}
)

// Elasticsearch is an adaptor to connect a pipeline to
// an elasticsearch cluster.
type Elasticsearch struct {
	client client.Writer

	index     string
	typeMatch *regexp.Regexp

	pipe *pipe.Pipe
	path string
}

// Description for the Elasticsearcb adaptor
func (e *Elasticsearch) Description() string {
	return description
}

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
			return nil, adaptor.Error{
				Lvl:    adaptor.CRITICAL,
				Path:   path,
				Err:    fmt.Sprintf("bad config (%s)", err.Error()),
				Record: nil,
			}
		}
		log.With("path", path).Debugf("adaptor config: %+v", conf)

		e := &Elasticsearch{
			pipe: p,
			path: path,
		}

		e.index, e.typeMatch, err = extra.CompileNamespace()
		if err != nil {
			return e, adaptor.Error{
				Lvl:    adaptor.CRITICAL,
				Path:   path,
				Err:    fmt.Sprintf("can't split namespace into index and typeMatch (%s)", err.Error()),
				Record: nil,
			}
		}

		err = e.setupClient(conf)
		return e, err
	})
}

// Start the adaptor as a source (not implemented)
func (e *Elasticsearch) Start() error {
	return errors.New("Start is unsupported for elasticsearch")
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

	if c, ok := e.client.(client.Closer); ok {
		c.Close()
	}

	log.With("path", e.path).Infoln("adaptor Stopped")
	return nil
}

func (e *Elasticsearch) applyOp(msg message.Msg) (message.Msg, error) {
	msgCopy := make(map[string]interface{})
	// Copy from the original map to the target map
	for key, value := range msg.Data() {
		msgCopy[key] = value
	}
	err := e.client.Write(message.From(msg.OP(), msg.Namespace(), msgCopy))(nil)

	if err != nil {
		e.pipe.Err <- adaptor.Error{
			Lvl:    adaptor.ERROR,
			Path:   e.path,
			Err:    fmt.Sprintf("write message error (%s)", err),
			Record: msg.Data,
		}
	}
	return msg, err
}

func (e *Elasticsearch) setupClient(conf Config) error {
	uri, err := url.Parse(conf.URI)
	if err != nil {
		return client.InvalidURIError{URI: conf.URI, Err: err.Error()}
	}

	hostsAndPorts := strings.Split(uri.Host, ",")
	stringVersion, err := determineVersion(fmt.Sprintf("%s://%s", uri.Scheme, hostsAndPorts[0]), uri.User)
	if err != nil {
		return err
	}

	v, err := version.NewVersion(stringVersion)
	if err != nil {
		return client.VersionError{URI: conf.URI, V: stringVersion, Err: err.Error()}
	}

	timeout, err := time.ParseDuration(conf.Timeout)
	if err != nil {
		log.Infof("failed to parse duration, %s, falling back to default timeout of 30s", conf.Timeout)
		timeout = 30 * time.Second
	}

	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: newTransport(conf.AWSAccessKeyID, conf.AWSAccessSecret),
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
				Index:      e.index,
			}
			versionedClient, _ := vc.Creator(opts)
			e.client = versionedClient
			return nil
		}
	}

	return client.VersionError{URI: conf.URI, V: stringVersion, Err: "unsupported client"}
}

func determineVersion(uri string, user *url.Userinfo) (string, error) {
	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return "", err
	}
	if user != nil {
		if pwd, ok := user.Password(); ok {
			req.SetBasicAuth(user.Username(), pwd)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", client.ConnectError{Reason: uri}
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", client.VersionError{URI: uri, V: "", Err: "unable to read response body"}
	}
	defer resp.Body.Close()
	var r struct {
		Name    string `json:"name"`
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if resp.StatusCode != http.StatusOK {
		return "", client.VersionError{URI: uri, V: "", Err: fmt.Sprintf("bad status code: %d", resp.StatusCode)}
	}
	err = json.Unmarshal(body, &r)
	if err != nil {
		return "", client.VersionError{URI: uri, V: "", Err: fmt.Sprintf("malformed JSON: %s", body)}
	} else if r.Version.Number == "" {
		return "", client.VersionError{URI: uri, V: "", Err: fmt.Sprintf("missing version: %s", body)}
	}
	return r.Version.Number, nil
}

// Config provides configuration options for an elasticsearch adaptor
// the notable difference between this and dbConfig is the presence of the Timeout option
type Config struct {
	URI             string `json:"uri" doc:"the uri to connect to, in the form mongodb://user:password@host.com:27017/auth_database"`
	Namespace       string `json:"namespace" doc:"mongo namespace to read/write"`
	Timeout         string `json:"timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	AWSAccessKeyID  string `json:"aws_access_key" doc:"credentials for use with AWS Elasticsearch service"`
	AWSAccessSecret string `json:"aws_access_secret" doc:"credentials for use with AWS Elasticsearch service"`
}
