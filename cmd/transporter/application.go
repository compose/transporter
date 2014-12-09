package main

import (
	"fmt"

	"github.com/compose/transporter/pkg/transporter"
)

type application struct {
	Config    Config
	Pipelines []*transporter.Pipeline
}

func Application(config Config) *application {
	return &application{Pipelines: make([]*transporter.Pipeline, 0), Config: config}
}

func (t *application) AddPipeline(p *transporter.Pipeline) {
	t.Pipelines = append(t.Pipelines, p)
}

// Run performs a .Run() on each Pipeline contained in the Transporter application
func (t *application) Run() (err error) {

	for _, p := range t.Pipelines {
		err = p.Run()
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *application) Stop() error {
	return nil
}

// represent this as a string
func (t *application) String() string {
	out := "TransporterApplication:\n"
	for _, p := range t.Pipelines {
		out += fmt.Sprintf("%s", p.String())
	}
	return out
}
