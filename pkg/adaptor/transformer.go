package adaptor

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

// Transformer is an adaptor which consumes data from a source, transforms it using a supplied javascript
// function and then emits it.  The javascript transformation function is supplied as a seperate file on disk,
// and is called by calling the defined module.exports function
type Transformer struct {
	fn string

	pipe *pipe.Pipe
	path string

	debug  bool
	script *otto.Script
	vm     *otto.Otto
}

// NewTransformer creates a new transformer object
func NewTransformer(pipe *pipe.Pipe, path string, extra Config) (StopStartListener, error) {
	var (
		conf TransformerConfig
		err  error
	)
	if err = extra.Construct(&conf); err != nil {
		return nil, err
	}

	t := &Transformer{pipe: pipe, path: path}

	if conf.Filename == "" {
		return t, fmt.Errorf("No filename specified")
	}

	ba, err := ioutil.ReadFile(conf.Filename)
	if err != nil {
		return t, err
	}

	t.fn = string(ba)

	return t, nil
}

// Listen starts the transformer's listener, reads each message from the incoming channel
// transformers it into mejson, and then uses the supplied javascript module.exports function
// to transform the document.  The document is then emited to this adaptor's children
func (t *Transformer) Listen() (err error) {
	if err = t.initEnvironment(); err != nil {
		return err
	}

	return t.pipe.Listen(t.transformOne)
}

// initEvironment prepares the javascript vm and compiles the transformer script
func (t *Transformer) initEnvironment() (err error) {
	t.vm = otto.New()

	// set up the vm environment, make `module = {}`
	if _, err = t.vm.Run(`module = {}`); err != nil {
		return t.transformerError(CRITICAL, err, nil)
	}

	// compile our script
	if t.script, err = t.vm.Compile("", t.fn); err != nil {
		return t.transformerError(CRITICAL, err, nil)
	}

	// run the script, ignore the output
	_, err = t.vm.Run(t.script)
	if err != nil {
		return t.transformerError(CRITICAL, err, nil)
	}
	return
}

// Start the adaptor as a source (not implemented for this adaptor)
func (t *Transformer) Start() error {
	return fmt.Errorf("Transformers can't be used as a source")
}

// Stop the adaptor
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
	if msg.IsMap() {
		if doc, err = mejson.Marshal(msg.Data); err != nil {
			t.pipe.Err <- t.transformerError(ERROR, err, msg)
			return msg, nil
		}
	} else {
		doc = msg.Data
	}

	if value, err = t.vm.ToValue(doc); err != nil {
		t.pipe.Err <- t.transformerError(ERROR, err, msg)
		return msg, nil
	}

	// now that we have finished casting our map to a bunch of different types,
	// lets run our transformer on the document
	beforeVM := time.Now().Nanosecond()
	if outDoc, err = t.vm.Call(`module.exports`, nil, value); err != nil {
		t.pipe.Err <- t.transformerError(ERROR, err, msg)
		return msg, nil
	}

	if result, err = outDoc.Export(); err != nil {
		t.pipe.Err <- t.transformerError(ERROR, err, msg)
		return msg, nil
	}

	afterVM := time.Now().Nanosecond()

	switch r := result.(type) {
	case map[string]interface{}:
		doc, err := mejson.Unmarshal(r)
		if err != nil {
			t.pipe.Err <- t.transformerError(ERROR, err, msg)
			return msg, nil
		}
		msg.Data = map[string]interface{}(doc)
	default:
		msg.Data = r
	}

	if t.debug {
		then := time.Now().Nanosecond()
		fmt.Printf("document transformed in %dus.  %d to marshal, %d in the vm, %d to unmarshal\n", (then-now)/1000, (beforeVM-now)/1000, (afterVM-beforeVM)/1000, (then-afterVM)/1000)
	}

	return msg, nil
}

func (t *Transformer) transformerError(lvl ErrorLevel, err error, msg *message.Msg) error {
	var data interface{}
	if msg != nil {
		data = msg.Data
	}

	if e, ok := err.(*otto.Error); ok {
		return NewError(lvl, t.path, fmt.Sprintf("Transformer error (%s)", e.String()), data)
	}
	return NewError(lvl, t.path, fmt.Sprintf("Transformer error (%s)", err.Error()), data)
}

// TransformerConfig holds config options for a transformer adaptor
type TransformerConfig struct {
	// file containing transformer javascript
	// must define a module.exports = function(doc) { .....; return doc }
	Filename string `json:"filename" doc:"the filename containing the javascript transform fn"`

	// verbose output
	Debug bool `json:"debug" doc:"display debug information"` // debug mode
}
