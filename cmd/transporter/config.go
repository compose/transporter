package main

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"regexp"
	"time"
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
		URI             string `json:"uri" yaml:"uri"`           // Uri of session store
		SessionInterval string `json:"interval" yaml:"interval"` // how often to persist the sesion states
		Type            string `json:"type" yaml:"type"`         // the type of SessionStore to use
	} `json:"sessions" yaml:"sessions"`
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

	// configs can have environment variables, replace these before continuing
	ba = setConfigEnvironment(ba)

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

// setConfigEnvironment replaces environment variables marked in the form ${FOO} with the
// value stored in the environment variable `FOO`
func setConfigEnvironment(ba []byte) []byte {
	re := regexp.MustCompile(`\$\{([a-zA-Z0-9_]+)\}`)

	matches := re.FindAllSubmatch(ba, -1)
	if matches == nil {
		return ba
	}

	for _, m := range matches {
		v := os.Getenv(string(m[1]))
		ba = bytes.Replace(ba, m[0], []byte(v), -1)
	}

	return ba
}
