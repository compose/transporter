package file

import (
	"fmt"
	"regexp"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/pipe"
)

const (
	sampleConfig = `    type: file
    uri: stdout://`

	description = "an adaptor that reads / writes files"
)

var (
	_ adaptor.Adaptor = &File{}
)

// Config is used to configure the File Adaptor
type Config struct {
	URI string `json:"uri" doc:"the uri to connect to, ie stdout://, file:///tmp/output"`
}

// File is an adaptor that can be used as a
// source / sink for file's on disk, as well as a sink to stdout.
type File struct {
	uri         string
	pipe        *pipe.Pipe
	path        string
	client      client.Client
	writer      client.Writer
	reader      client.Reader
	doneChannel chan struct{}
}

func init() {
	adaptor.Add("file", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf Config
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, adaptor.Error{
				Lvl:    adaptor.CRITICAL,
				Path:   path,
				Err:    fmt.Sprintf("Can't configure adaptor (%s)", err.Error()),
				Record: nil,
			}
		}

		f := &File{
			uri:         conf.URI,
			pipe:        p,
			path:        path,
			writer:      newWriter(),
			reader:      newReader(),
			doneChannel: make(chan struct{}),
		}

		f.client, err = NewClient(WithURI(conf.URI))
		return f, err
	})
}

// Description for file adaptor
func (f *File) Description() string {
	return description
}

// SampleConfig for file adaptor
func (f *File) SampleConfig() string {
	return sampleConfig
}

// Start the file adaptor
func (f *File) Start() (err error) {
	log.With("file", f.uri).Infoln("adaptor Starting...")
	defer func() {
		f.pipe.Stop()
	}()

	s, err := f.client.Connect()
	if err != nil {
		return err
	}
	readFunc := f.reader.Read(func(string) bool { return true })
	msgChan, err := readFunc(s, f.doneChannel)
	if err != nil {
		return err
	}
	for msg := range msgChan {
		f.pipe.Send(msg)
	}

	log.With("file", f.uri).Infoln("adaptor Start finished...")
	return nil
}

// Listen starts the listener
func (f *File) Listen() error {
	log.With("file", f.uri).Infoln("adaptor Listening...")
	defer func() {
		log.With("file", f.uri).Infoln("adaptor Listen closing...")
		f.pipe.Stop()
	}()
	return f.pipe.Listen(f.applyOp, regexp.MustCompile(`.*`))
}

func (f *File) applyOp(msg message.Msg) (message.Msg, error) {
	err := client.Write(f.client, f.writer, message.From(msg.OP(), msg.Namespace(), msg.Data()))
	if err != nil {
		f.pipe.Err <- adaptor.Error{
			Lvl:    adaptor.ERROR,
			Path:   f.path,
			Err:    fmt.Sprintf("write message error (%s)", err),
			Record: msg.Data(),
		}
	}
	return msg, err
}

// Stop the adaptor
func (f *File) Stop() error {
	f.pipe.Stop()
	if c, ok := f.client.(client.Closer); ok {
		c.Close()
	}
	return nil
}
