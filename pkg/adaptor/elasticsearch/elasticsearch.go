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
	version "github.com/hashicorp/go-version"
)

const (
	description  = "an elasticsearch sink adaptor"
	sampleConfig = `
- es:
		type: elasticsearch
    uri: https://username:password@hostname:port
		timeout: 10s # optional, defaults to 30s
		aws_access_key: XXX # optional, used for signing requests to AWS Elasticsearch service
		aws_access_secret: XXX # optional, used for signing requests to AWS Elasticsearch service
`
)

var (
	_ adaptor.Adaptor = &Elasticsearch{}
)

// InvalidURIError wraps the underlying error when the provided URI is not parsable by url.Parse.
type InvalidURIError struct {
	uri string
}

func (e InvalidURIError) Error() string {
	return fmt.Sprintf("Invalid URI, %s", e.uri)
}

// ConnectionError wraps any failed calls to the provided uri.
type ConnectionError struct {
	uri string
}

func (e ConnectionError) Error() string {
	return fmt.Sprintf("failed to connect to %s", e.uri)
}

// VersionError represents any failure in attempting to obtain the version from the provided uri.
type VersionError struct {
	uri string
	v   string
	err string
}

func (e VersionError) Error() string {
	if e.v == "" {
		return fmt.Sprintf("unable to determine version from %s, %s", e.uri, e.err)
	}
	return fmt.Sprintf("%s running %s, %s", e.uri, e.v, e.err)
}

// InvalidTimeoutError wraps the underlying error when the provided is not parsable time.ParseDuration
// type InvalidTimeoutError struct {
// 	timeout string
// }
//
// func (e InvalidTimeoutError) Error() string {
// 	return fmt.Sprintf("Invalid Timeout, %s", e.timeout)
// }

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

	conf Config
}

// Description for the Elasticsearcb adaptor
func (e *Elasticsearch) Description() string {
	return description
}

// Migrate migrates a elasticsearch based on the schema directive
func (e *Elasticsearch) Migrate() error {
	log.Infoln("migrating elasticsearch... %s", e.conf.Schema)
	return nil
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
			return nil, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("bad config (%s)", err.Error()), nil)
		}
		log.With("path", path).Debugf("adaptor config: %+v", conf)

		e := &Elasticsearch{
			pipe:        p,
			path:        path,
			conf:        conf,
			doneChannel: make(chan struct{}),
		}

		e.index, e.typeMatch, err = extra.CompileNamespace()
		if err != nil {
			return e, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("can't split namespace into index and typeMatch (%s)", err.Error()), nil)
		}

		if err := e.setupClient(conf); err != nil {
			return nil, err
		}

		return e, nil
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

	close(e.doneChannel)
	e.wg.Wait()

	log.With("path", e.path).Infoln("adaptor Stopped")
	return nil
}

func (e *Elasticsearch) applyOp(msg message.Msg) (message.Msg, error) {
	_, msgColl, _ := message.SplitNamespace(msg)
	msgCopy := make(map[string]interface{})
	// Copy from the original map to the target map
	for key, value := range msg.Data() {
		msgCopy[key] = value
	}
	err := e.client.Write(message.From(msg.OP(), e.computeNamespace(msgColl), msgCopy))(nil)

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
		return InvalidURIError{conf.URI}
	}

	hostsAndPorts := strings.Split(uri.Host, ",")
	stringVersion, err := determineVersion(fmt.Sprintf("%s://%s", uri.Scheme, hostsAndPorts[0]))
	if err != nil {
		return err
	}

	v, err := version.NewVersion(stringVersion)
	if err != nil {
		return VersionError{conf.URI, stringVersion, err.Error()}
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
			}
			versionedClient, _ := vc.Creator(e.doneChannel, &e.wg, opts)
			e.client = versionedClient
			return nil
		}
	}

	return VersionError{conf.URI, stringVersion, "unsupported client"}
}

func determineVersion(uri string) (string, error) {
	resp, err := http.DefaultClient.Get(uri)
	if err != nil {
		return "", ConnectionError{uri}
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", VersionError{uri, "", "unable to read response body"}
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
		return "", VersionError{uri, "", fmt.Sprintf("malformed JSON: %s", body)}
	} else if r.Version.Number == "" {
		return "", VersionError{uri, "", fmt.Sprintf("missing version: %s", body)}
	}
	return r.Version.Number, nil
}

// Config provides configuration options for an elasticsearch adaptor
// the notable difference between this and dbConfig is the presence of the Timeout option
type Config struct {
	adaptor.BaseConfig
	Timeout         string `json:"timeout" doc:"timeout for establishing connection, format must be parsable by time.ParseDuration and defaults to 10s"`
	AWSAccessKeyID  string `json:"aws_access_key" doc:"credentials for use with AWS Elasticsearch service"`
	AWSAccessSecret string `json:"aws_access_secret" doc:"credentials for use with AWS Elasticsearch service"`
}
