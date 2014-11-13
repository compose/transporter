package transporter

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/compose/mejson"
	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/pipe"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore" // enable underscore
)

type Transformer struct {
	name string `json:"name"`
	Func string `json:"func"`

	pipe pipe.Pipe

	debug  bool
	script *otto.Script
	vm     *otto.Otto
}

func NewTransformer(name, namespace, uri string, role NodeRole, extra map[string]interface{}) (*Transformer, error) {
	t := &Transformer{}

	filename, ok := extra["filename"].(string)
	if !ok {
		return t, fmt.Errorf("No filename specified")
	}
	ba, err := ioutil.ReadFile(filename)
	if err != nil {
		return t, err
	}
	t.name = filename
	t.Func = string(ba)

	return t, nil
}

func (t *Transformer) String() string {
	return fmt.Sprintf("%-20s %-15s", t.name, "Transformer")
}

func (t *Transformer) Start(pipe pipe.Pipe) (err error) {
	t.pipe = pipe

	t.vm = otto.New()

	// set up the vm environment, make `module = {}`
	if _, err = t.vm.Run(`module = {}`); err != nil {
		return err
	}

	// compile our script
	if t.script, err = t.vm.Compile("", t.Func); err != nil {
		return err
	}

	// run the script, ignore the output
	_, err = t.vm.Run(t.script)
	if err != nil {
		return err
	}

	return t.pipe.Listen(t.transformOne)
}

func (t *Transformer) Stop() error {
	t.pipe.Stop()
	return nil
}

func (t *Transformer) Name() string {
	return t.name
}

func (t *Transformer) Type() string {
	return "transformer"
}

func (t *Transformer) transformOne(msg *message.Msg) error {

	var (
		doc    interface{}
		value  otto.Value
		outDoc otto.Value
		result interface{}
		err    error
	)

	// short circuit for deletes and commands
	if msg.Op == message.Delete || msg.Op == message.Command {
		t.pipe.Send(msg)
		return nil
	}

	now := time.Now().Nanosecond()

	if doc, err = mejson.Marshal(msg.Document()); err != nil {
		return err
	}

	if value, err = t.vm.ToValue(doc); err != nil {
		return err
	}

	// now that we have finished casting our map to a bunch of different types,
	// lets run our transformer on the document
	beforeVM := time.Now().Nanosecond()
	if outDoc, err = t.vm.Call(`module.exports`, nil, value); err != nil {
		return err
	}

	if result, err = outDoc.Export(); err != nil {
		return err
	}

	afterVM := time.Now().Nanosecond()

	switch r := result.(type) {
	case map[string]interface{}:
		doc, err := mejson.Unmarshal(r)
		if err != nil {
			return err
		}
		msg.SetDocument(doc)
		t.pipe.Send(msg)
	default:
		if t.debug {
			fmt.Println("transformer skipping doc")
		}
	}

	if t.debug {
		then := time.Now().Nanosecond()
		fmt.Printf("document transformed in %dus.  %d to marshal, %d in the vm, %d to unmarshal\n", (then-now)/1000, (beforeVM-now)/1000, (afterVM-beforeVM)/1000, (then-afterVM)/1000)
	}

	return nil
}
