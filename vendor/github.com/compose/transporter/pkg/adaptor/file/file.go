package file

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/adaptor/file"
	"github.com/compose/transporter/pkg/message/ops"
	"github.com/compose/transporter/pkg/pipe"
)

// File is an adaptor that can be used as a
// source / sink for file's on disk, as well as a sink to stdout.
type File struct {
	uri  string
	pipe *pipe.Pipe
	path string
}

// Description for file adaptor
func (f *File) Description() string {
	return "an adaptor that reads / writes files"
}

const sampleConfig = `
- stdout:
    type: file
    uri: stdout://
`

// SampleConfig for file adaptor
func (f *File) SampleConfig() string {
	return sampleConfig
}

func init() {
	adaptor.Add("file", adaptor.Creator(func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.Adaptor, error) {
		var (
			conf Config
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("Can't configure adaptor (%s)", err.Error()), nil)
		}

		return &File{
			uri:  conf.URI,
			pipe: p,
			path: path,
		}, nil
	}))
}

// Start the file adaptor
// TODO: we only know how to listen on stdout for now
func (f *File) Start() (err error) {
	defer func() {
		f.Stop()
	}()

	return f.readFile()
}

// Listen starts the listen loop
func (f *File) Listen() error {
	defer func() {
		f.Stop()
	}()

	if strings.HasPrefix(f.uri, "file://") {
		name := strings.Replace(f.uri, "file://", "", 1)
		_, err := os.Create(name)
		if err != nil {
			f.pipe.Err <- adaptor.NewError(adaptor.CRITICAL, f.path, fmt.Sprintf("Can't open output file (%s)", err.Error()), nil)
			return err
		}
	}

	return f.pipe.Listen(f.dumpMessage, regexp.MustCompile(`.*`))
}

// Stop the adaptor
func (f *File) Stop() error {
	f.pipe.Stop()
	return nil
}

// read each message from the file
func (f *File) readFile() error {
	name := strings.Replace(f.uri, "file://", "", 1)
	fh, err := os.Open(name)
	if err != nil {
		f.pipe.Err <- adaptor.NewError(adaptor.CRITICAL, f.path, fmt.Sprintf("Can't open input file (%s)", err.Error()), nil)
		return err
	}

	decoder := json.NewDecoder(fh)
	for {
		var doc map[string]interface{}
		if err := decoder.Decode(&doc); err == io.EOF {
			break
		}
		if err != nil {
			f.pipe.Err <- adaptor.NewError(adaptor.ERROR, f.path, fmt.Sprintf("Can't marshal document (%s)", err.Error()), nil)
			return err
		}
		f.pipe.Send(message.MustUseAdaptor("file").From(ops.Insert, fmt.Sprintf("file.%s", name), doc))
	}
	return nil
}

/*
 * dump each message to the file
 */
func (f *File) dumpMessage(msg message.Msg) (message.Msg, error) {
	return message.Exec(message.MustUseAdaptor("file").(file.Adaptor).MustUseFile(f.uri), msg)
}

// Config is used to configure the File Adaptor
type Config struct {
	// URI pointing to the resource.  We only recognize file:// and stdout:// currently
	URI string `json:"uri" doc:"the uri to connect to, ie stdout://, file:///tmp/output"`
}
