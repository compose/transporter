package elasticsearch

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/adaptor/elasticsearch/clients"
	// used to call init function for each client to register itself
	_ "github.com/compose/transporter/adaptor/elasticsearch/clients/all"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	version "github.com/hashicorp/go-version"
)

const (
	description  = "an elasticsearch sink adaptor"
	sampleConfig = `    type: elasticsearch
    uri: ${ELASTICSEARCH_URI}
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
	adaptor.BaseConfig
	AWSAccessKeyID  string `json:"aws_access_key" doc:"credentials for use with AWS Elasticsearch service"`
	AWSAccessSecret string `json:"aws_access_secret" doc:"credentials for use with AWS Elasticsearch service"`
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
	adaptor.Add(
		"elasticsearch",
		func() adaptor.Adaptor {
			return &Elasticsearch{}
		},
	)
}

// Client returns a client that doesn't do anything other than fulfill the client.Client interface.
func (e *Elasticsearch) Client() (client.Client, error) {
	return &client.Mock{}, nil
}

// Reader returns an error because this adaptor is currently not supported as a Source.
func (e *Elasticsearch) Reader() (client.Reader, error) {
	return nil, adaptor.ErrFuncNotSupported{Name: "Reader()", Func: "elasticsearch"}
}

// Writer determines the which underlying writer to used based on the cluster's version.
func (e *Elasticsearch) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	index, _, _ := adaptor.CompileNamespace(e.Namespace)
	return setupWriter(index, e)
}

func setupWriter(index string, conf *Elasticsearch) (client.Writer, error) {
	uri, err := url.Parse(conf.URI)
	if err != nil {
		return nil, client.InvalidURIError{URI: conf.URI, Err: err.Error()}
	}

	hostsAndPorts := strings.Split(uri.Host, ",")
	stringVersion, err := determineVersion(fmt.Sprintf("%s://%s", uri.Scheme, hostsAndPorts[0]), uri.User)
	if err != nil {
		return nil, err
	}

	v, err := version.NewVersion(stringVersion)
	if err != nil {
		return nil, client.VersionError{URI: conf.URI, V: stringVersion, Err: err.Error()}
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
				Index:      index,
			}
			versionedClient, _ := vc.Creator(opts)
			return versionedClient, nil
		}
	}

	return nil, client.VersionError{URI: conf.URI, V: stringVersion, Err: "unsupported client"}
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
