package node

import (
	"encoding/json"
	"fmt"

	"github.com/MongoHQ/transporter/pkg/message"
)

type FileImpl struct {
	pipe Pipe
}

func NewFileImpl(Name, Type, Uri, Namespace string) (*FileImpl, error) {
	return &FileImpl{}, nil
}

/*
 * start the module
 * TODO: we only know how to listen on stdout for now
 */

func (d *FileImpl) Start(pipe Pipe) error {
	d.pipe = pipe

	return d.pipe.Listen(d.debugMessage)
}

/*
 * stop the capsule
 */
func (d *FileImpl) Stop() error {
	d.pipe.Stop()
	return nil
}

/*
 * perform action on each message
 */
func (d *FileImpl) debugMessage(msg *message.Msg) error {

	jdoc, err := json.Marshal(msg.Document())
	if err != nil {
		return fmt.Errorf("can't unmarshal doc %v", err)
	}
	fmt.Println(string(jdoc))

	return nil
}
