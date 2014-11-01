package main

import (
	"io/ioutil"

	"github.com/MongoHQ/transporter/pkg/node"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Nodes []node.Node
}

/*
 *
 * Load Config file from disk
 *
 */
func (a *ApplicationBuilder) loadConfig() (err error) {
	var c Config
	if a.config_path == "" {
		a.Config = c
		return nil
	}

	ba, err := ioutil.ReadFile(a.config_path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(ba, &c)
	a.Config = c

	return err
}
