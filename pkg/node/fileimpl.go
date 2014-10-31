package node

import (
	"encoding/json"
	"fmt"

	"github.com/MongoHQ/transporter/pkg/message"
)

type FileImpl struct {
	pipe      Pipe
	role      NodeRole
	uri       string
	name      string
	kind      string
	namespace string
}

func NewFileImpl(role NodeRole, name, kind, uri, namespace string) (*FileImpl, error) {
	return &FileImpl{name: name, kind: kind, uri: uri, namespace: namespace, role: role}, nil
}

/*
 * start the module
 * TODO: we only know how to listen on stdout for now
 */

func (d *FileImpl) Start(pipe Pipe) error {
	d.pipe = pipe

	if d.role == SINK {
		return d.pipe.Listen(d.debugMessage)
	} else {
		return fmt.Errorf("file as a source is not yet implemented")
	}
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
