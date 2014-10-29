package application

import (
	"fmt"

	"github.com/MongoHQ/transporter/pkg/node"
)

type TransporterApplication struct {
	Pipelines []node.Pipeline
}

func NewTransporterApplication() *TransporterApplication {
	return &TransporterApplication{Pipelines: make([]node.Pipeline, 0)}
}

func (t *TransporterApplication) AddPipeline(p node.Pipeline) {
	t.Pipelines = append(t.Pipelines, p)
}

func (t *TransporterApplication) Run() error {
	fmt.Println(t)
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
