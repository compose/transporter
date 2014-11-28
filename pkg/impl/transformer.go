package impl

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
	Func string

	pipe *pipe.Pipe

	debug  bool
	script *otto.Script
	vm     *otto.Otto
}

func NewTransformer(p *pipe.Pipe, extra ExtraConfig) (*Transformer, error) {
	var (
		conf TransformerConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	t := &Transformer{pipe: p}

	if conf.Filename == "" {
		return t, fmt.Errorf("No filename specified")
	}

	ba, err := ioutil.ReadFile(conf.Filename)
	if err != nil {
		return t, err
	}

	t.Func = string(ba)

	return t, nil
}

func (t *Transformer) Listen() (err error) {
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

func (t *Transformer) Start() error {
	return fmt.Errorf("Transformers can't be used as a source")
}

func (t *Transformer) Stop() error {
	t.pipe.Stop()
	return nil
}

func (t *Transformer) transformOne(msg *message.Msg) (*message.Msg, error) {

	var (
		doc    interface{}
		value  otto.Value
		outDoc otto.Value
		result interface{}
		err    error
	)

	// short circuit for deletes and commands
	if msg.Op == message.Delete || msg.Op == message.Command {
		return msg, nil
	}

	now := time.Now().Nanosecond()

	if doc, err = mejson.Marshal(msg.Document()); err != nil {
		return msg, err
	}

	if value, err = t.vm.ToValue(doc); err != nil {
		return msg, err
	}

	// now that we have finished casting our map to a bunch of different types,
	// lets run our transformer on the document
	beforeVM := time.Now().Nanosecond()
	if outDoc, err = t.vm.Call(`module.exports`, nil, value); err != nil {
		return msg, err
	}

	if result, err = outDoc.Export(); err != nil {
		return msg, err
	}

	afterVM := time.Now().Nanosecond()

	switch r := result.(type) {
	case map[string]interface{}:
		doc, err := mejson.Unmarshal(r)
		if err != nil {
			return msg, err
		}
		msg.SetDocument(doc)
		// t.pipe.Send(msg)
	default:
		if t.debug {
			fmt.Println("transformer skipping doc")
		}
	}

	if t.debug {
		then := time.Now().Nanosecond()
		fmt.Printf("document transformed in %dus.  %d to marshal, %d in the vm, %d to unmarshal\n", (then-now)/1000, (beforeVM-now)/1000, (afterVM-beforeVM)/1000, (then-afterVM)/1000)
	}

	return msg, nil
}

// InfluxdbConfig options
type TransformerConfig struct {
	Filename string `json:"filename"` // file containing transformer javascript
	Debug    bool   `json:"debug"`    // debug mode
}
