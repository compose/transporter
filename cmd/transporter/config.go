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
	API struct {
		URI             string `json:"uri" yaml:"uri"`           // Uri to connect to
		MetricsInterval string `json:"interval" yaml:"interval"` // how often to emit metrics, (in ms)
		Key             string `json:"key" yaml:"key"`           // http basic auth password to send with each event
		Pid             string `json:"pid" yaml:"pid"`           // http basic auth username to send with each event
	} `json:"api" yaml:"api"`
	Sessions struct {
		URI             string `json:"uri" yaml:"uri"`           //Uri of session store
		SessionInterval string `json:"interval" yaml:"interval"` // how often to persist the sesion states
		Type            string `json:"type" yaml:"type"`         // the type of SessionStore to use
	}
	Nodes map[string]map[string]interface{}
}

// LoadConfig loads a config yaml from a file on disk.
// if the pid is not set in the yaml, pull it from the environment TRANSPORTER_PID.
// if that env var isn't present, then generate a pid
func LoadConfig(filename string) (config Config, err error) {
	if filename == "" {
		if _, err := os.Stat("config.yaml"); os.IsNotExist(err) {
			return config, nil // return the default config
		}
		filename = "config.yaml"
	}

	ba, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	err = yaml.Unmarshal(ba, &config)

	for k, v := range config.Nodes {
		config.Nodes[k] = v
	}

	if len(config.API.Pid) < 1 {
		config.API.Pid = os.Getenv("TRANSPORTER_PID")
	}

	if len(config.API.Pid) < 1 {
		hostname, _ := os.Hostname()
		config.API.Pid = fmt.Sprintf("%s@%d", hostname, time.Now().Unix())
	}

	return
}
