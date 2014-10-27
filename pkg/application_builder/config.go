package application_builder

import (
	"errors"
	"io/ioutil"

	"github.com/MongoHQ/transporter/pkg/node"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Nodes []node.NodeConfig
}

/*
 *
 * Load Config file from disk
 *
 */
func (a *ApplicationBuilder) loadConfig() (err error) {
	var c Config
	if a.config_path == "" {
		return errors.New("missing config file")
	}

	ba, err := ioutil.ReadFile(a.config_path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(ba, &c)
	a.Config = c

	return err
}
