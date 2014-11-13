package transporter

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
)

type FileImpl struct {
	name   string
	uri    string
	pipe   pipe.Pipe
	config ConfigNode

	filehandle *os.File
}

func NewFileImpl(c ConfigNode) (*FileImpl, error) {
	return &FileImpl{config: c}, nil

}

/*
 * start the module
 * TODO: we only know how to listen on stdout for now
 */

func (d *FileImpl) Start(pipe pipe.Pipe) (err error) {
	d.pipe = pipe
	defer func() {
		d.Stop()
	}()

	if d.config.Role == SINK {
		if strings.HasPrefix(d.uri, "file://") {
			filename := strings.Replace(d.uri, "file://", "", 1)
			d.filehandle, err = os.Create(filename)
			if err != nil {
				d.pipe.Err <- err
				return err
			}
		}

		return d.pipe.Listen(d.dumpMessage)
	} else {
		return d.readFile()
	}
}

/*
 * stop the capsule
 */
func (d *FileImpl) Stop() error {
	d.pipe.Stop()
	return nil
}

func (d *FileImpl) String() string {
	return fmt.Sprintf("%-20s %-15s %-30s %s", d.name, "file", "", d.uri)
}

/*
 * read each message from the file
 */
func (d *FileImpl) readFile() (err error) {
	filename := strings.Replace(d.uri, "file://", "", 1)
	d.filehandle, err = os.Open(filename)
	if err != nil {
		d.pipe.Err <- err
		return err
	}

	decoder := json.NewDecoder(d.filehandle)
	for {
		var doc map[string]interface{}
		if err := decoder.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			d.pipe.Err <- err
			return err
		}
		d.pipe.Send(message.NewMsg(message.Insert, d.uri, doc))
	}
	return nil
}

/*
 * dump each message to the file
 */
func (d *FileImpl) dumpMessage(msg *message.Msg) error {
	jdoc, err := json.Marshal(msg.Document())
	if err != nil {
		return fmt.Errorf("can't unmarshal doc %v", err)
	}

	if strings.HasPrefix(d.config.Uri, "stdout://") {
		fmt.Println(string(jdoc))
	} else {
		_, err = fmt.Fprintln(d.filehandle, string(jdoc))
		if err != nil {
			return err
		}
	}

	return nil
}
