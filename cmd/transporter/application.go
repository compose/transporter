package main

import (
	"fmt"

	"github.com/compose/transporter/pkg/transporter"
)

type Application struct {
	Config    Config
	Pipelines []*transporter.Pipeline
}

func NewApplication(config Config) *Application {
	return &Application{Pipelines: make([]*transporter.Pipeline, 0), Config: config}
}

func (t *Application) AddPipeline(p *transporter.Pipeline) {
	t.Pipelines = append(t.Pipelines, p)
}

// Run performs a .Run() on each Pipeline contained in the Transporter Application
func (t *Application) Run() (err error) {

	for _, p := range t.Pipelines {
		err = p.Run()
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Application) Stop() error {
	return nil
}

// represent this as a string
func (t *Application) String() string {
	out := "TransporterApplication:\n"
	for _, p := range t.Pipelines {
		out += fmt.Sprintf("%s", p.String())
	}
	return out
}
