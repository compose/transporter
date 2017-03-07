package transformer

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	_ "github.com/robertkrimen/otto/underscore" // enable underscore
)

const (
	sampleConfig = `    filename: test/transformers/passthrough_and_log.js
    type: transformer`

	description = "an adaptor that transforms documents using a javascript function"
)

var (
	_ adaptor.Adaptor = &Transformer{}
)

// Transformer is an adaptor which consumes data from a source, transforms it using a supplied javascript
// function and then emits it. The javascript transformation function is supplied as a separate file on disk,
// and is called by calling the defined module.exports function
type Transformer struct {
	Filename string `json:"filename"`
}

func init() {
	adaptor.Add(
		"transformer",
		func() adaptor.Adaptor {
			return &Transformer{}
		},
	)
}

func (t *Transformer) Client() (client.Client, error) {
	return NewClient(WithFilename(t.Filename))
}

func (t *Transformer) Reader() (client.Reader, error) {
	return nil, adaptor.ErrFuncNotSupported{Name: "transformer", Func: "Reader()"}
}

func (t *Transformer) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	return &Writer{}, nil
}

// Description for transformer adaptor
func (t *Transformer) Description() string {
	return description
}

// SampleConfig for transformer adaptor
func (t *Transformer) SampleConfig() string {
	return sampleConfig
}
