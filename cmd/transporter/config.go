package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

// A Config stores meta information about the transporter.  This contains a
// list of the the nodes that are available to a transporter (sources and sinks, not transformers)
// as well as information about the api used to handle transporter events, and the interval
// between metrics events.
type Config struct {
	Api struct {
		Uri             string `json:"uri" yaml:"uri"`           // Uri to connect to
		MetricsInterval string `json:"interval" yaml:"interval"` // how often to emit metrics, (in ms)
		Key             string `json:"key" yaml:"key"`           // http basic auth password to send with each event
		Pid             string `json:"pid" yaml:"pid"`           // http basic auth username to send with each event
	} `json:"api" yaml:"api"`
	Nodes map[string]struct {
		Type string `json:"type" yaml:"type"`
		Uri  string `json:"uri" yaml:"uri"`
	}
}

func LoadConfig(filename string) (config Config, err error) {
	if filename == "" {
		return
	}

	ba, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	err = yaml.Unmarshal(ba, &config)

	for k, v := range config.Nodes {
		config.Nodes[k] = v
	}

	if len(config.Api.Pid) < 1 {
		config.Api.Pid = os.Getenv("TRANSPORTER_PID")
	}

	if len(config.Api.Pid) < 1 {
		hostname, _ := os.Hostname()
		config.Api.Pid = fmt.Sprintf("%s@%d", hostname, time.Now().Unix())
	}

	return
}
