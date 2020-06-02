package file

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
)

const (
	sampleConfig = `{
  "uri": "stdout://"
}`

	description = "an adaptor that reads / writes files"
)

// File is an adaptor that can be used as a
// source / sink for file's on disk, as well as a sink to stdout.
type File struct {
	adaptor.BaseConfig
}

func init() {
	adaptor.Add(
		"file",
		func() adaptor.Adaptor {
			return &File{}
		},
	)
}

// Client creates an instance of Client to be used for reading/writing to a file.
func (f *File) Client() (client.Client, error) {
	return NewClient(WithURI(f.URI))
}

// Reader instantiates a Reader for use with working with the file.
func (f *File) Reader() (client.Reader, error) {
	return newReader(), nil
}

// Writer instantiates a Writer for use with working with the file.
func (f *File) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return newWriter(), nil
}

// Description for file adaptor
func (f *File) Description() string {
	return description
}

// SampleConfig for file adaptor
func (f *File) SampleConfig() string {
	return sampleConfig
}
