package application

import (
	"fmt"

	"github.com/MongoHQ/transporter/pkg/node"
)

type TransporterApplication struct {
	Config    node.Config
	Pipelines []node.Pipeline
}

func NewTransporterApplication(config node.Config) *TransporterApplication {
	return &TransporterApplication{Pipelines: make([]node.Pipeline, 0), Config: config}
}

func (t *TransporterApplication) AddPipeline(p node.Pipeline) {
	t.Pipelines = append(t.Pipelines, p)
}

/*
 * This is where we actually instantiate the Pipelines
 */
func (t *TransporterApplication) Run() (err error) {
	fmt.Println(t)

	for _, p := range t.Pipelines {
		err = p.Create()
		if err != nil {
			return err
		}
	}

	for _, p := range t.Pipelines {
		err = p.Run()
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TransporterApplication) Stop() error {
	return nil
}

/*
 * represent this as a string
 */
func (t *TransporterApplication) String() string {
	out := "TransporterApplication:\n"
	for _, p := range t.Pipelines {
		out += fmt.Sprintf("%s", p.String())
	}
	return out
}
