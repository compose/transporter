package transformer

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"

	goja "github.com/compose/transporter/adaptor/transformer/gojajs"
	otto "github.com/compose/transporter/adaptor/transformer/ottojs"
)

const (
	sampleConfig = `    filename: transformer.js
    type: transformer
    # vm: otto`

	description = "an adaptor that transforms documents using a javascript function"

	// DefaultVM defines the javascript interpreter to be used if one is not specified
	DefaultVM = "otto"
)

var (
	_ adaptor.Adaptor = &Transformer{}
)

// Transformer is an adaptor which consumes data from a source, transforms it using a supplied javascript
// function and then emits it. The javascript transformation function is supplied as a separate file on disk,
// and is called by calling the defined module.exports function
type Transformer struct {
	Filename string `json:"filename"`
	VM       string `json:"vm"`
}

func init() {
	adaptor.Add(
		"transformer",
		func() adaptor.Adaptor {
			return &Transformer{
				VM: DefaultVM,
			}
		},
	)
}

func (t *Transformer) Client() (client.Client, error) {
	if t.VM == DefaultVM {
		return otto.NewClient(otto.WithFilename(t.Filename))
	}
	return goja.NewClient(goja.WithFilename(t.Filename))
}

func (t *Transformer) Reader() (client.Reader, error) {
	return nil, adaptor.ErrFuncNotSupported{Name: "transformer", Func: "Reader()"}
}

func (t *Transformer) Writer(chan struct{}, *sync.WaitGroup) (client.Writer, error) {
	if t.VM == DefaultVM {
		return &otto.Writer{}, nil
	}
	return &goja.Writer{}, nil
}

// Description for transformer adaptor
func (t *Transformer) Description() string {
	return description
}

// SampleConfig for transformer adaptor
func (t *Transformer) SampleConfig() string {
	return sampleConfig
}
