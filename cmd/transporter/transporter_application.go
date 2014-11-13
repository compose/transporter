package main

import (
	"fmt"

	"github.com/compose/transporter/pkg/transporter"
)

type TransporterApplication struct {
	Config    transporter.Config
	Pipelines []transporter.Pipeline
}

func NewTransporterApplication(config transporter.Config) *TransporterApplication {
	return &TransporterApplication{Pipelines: make([]transporter.Pipeline, 0), Config: config}
}

func (t *TransporterApplication) AddPipeline(p transporter.Pipeline) {
	t.Pipelines = append(t.Pipelines, p)
}

/*
 * This is where we actually instantiate the Pipelines
 */
func (t *TransporterApplication) Run() (err error) {
	fmt.Println(t)

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
