package transformer

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"time"

	"github.com/compose/mejson"
	"git.compose.io/compose/transporter/pkg/adaptor"
	"git.compose.io/compose/transporter/pkg/message"
	"git.compose.io/compose/transporter/pkg/pipe"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore" // enable underscore
)

// Transformer is an adaptor which consumes data from a source, transforms it using a supplied javascript
// function and then emits it. The javascript transformation function is supplied as a separate file on disk,
// and is called by calling the defined module.exports function
type Transformer struct {
	fn       string
	filename string

	pipe *pipe.Pipe
	path string
	ns   *regexp.Regexp

	debug  bool
	script *otto.Script
	vm     *otto.Otto
}

// Description for transformer adaptor
func (t *Transformer) Description() string {
	return "an adaptor that transforms documents using a javascript function"
}

var sampleConfig = `
- logtransformer:
    filename: test/transformers/passthrough_and_log.js
    type: transformer
`

// SampleConfig for transformer adaptor
func (t *Transformer) SampleConfig() string {
	return sampleConfig
}

func init() {
	adaptor.Add("transformer", func(p *pipe.Pipe, path string, extra adaptor.Config) (adaptor.StopStartListener, error) {
		var (
			conf Config
			err  error
		)
		if err = extra.Construct(&conf); err != nil {
			return nil, err
		}

		t := &Transformer{pipe: p, path: path, filename: conf.Filename}

		_, t.ns, err = extra.CompileNamespace()
		if err != nil {
			return t, adaptor.NewError(adaptor.CRITICAL, path, fmt.Sprintf("can't split transformer namespace (%s)", err.Error()), nil)
		}

		return t, nil
	})
}

// Connect loads the transformer file and initializes the transformer environment
func (t *Transformer) Connect() error {
	if t.filename == "" {
		return fmt.Errorf("no filename specified")
	}

	ba, err := ioutil.ReadFile(t.filename)
	if err != nil {
		return err
	}

	t.fn = string(ba)

	if err = t.initEnvironment(); err != nil {
		return err
	}
	return nil
}

// Listen starts the transformer's listener, reads each message from the incoming channel
// transformers it into mejson, and then uses the supplied javascript module.exports function
// to transform the document.  The document is then emitted to this adaptor's children
func (t *Transformer) Listen() (err error) {
	return t.pipe.Listen(t.transformOne, t.ns)
}

// initEvironment prepares the javascript vm and compiles the transformer script
func (t *Transformer) initEnvironment() (err error) {
	t.vm = otto.New()

	// set up the vm environment, make `module = {}`
	if _, err = t.vm.Run(`module = {}`); err != nil {
		return t.transformerError(adaptor.CRITICAL, err, nil)
	}

	// compile our script
	if t.script, err = t.vm.Compile("", t.fn); err != nil {
		return t.transformerError(adaptor.CRITICAL, err, nil)
	}

	// run the script, ignore the output
	_, err = t.vm.Run(t.script)
	if err != nil {
		return t.transformerError(adaptor.CRITICAL, err, nil)
	}
	return
}

// Start the adaptor as a source (not implemented for this adaptor)
func (t *Transformer) Start() error {
	return fmt.Errorf("transformers can't be used as a source")
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
	if msg.Op == message.Command {
		return msg, nil
	}

	now := time.Now().Nanosecond()
	currMsg := map[string]interface{}{
		"data": msg.Data,
		"ts":   msg.Timestamp,
		"op":   msg.Op.String(),
		"ns":   msg.Namespace,
	}
	if msg.IsMap() {
		if doc, err = mejson.Marshal(msg.Data); err != nil {
			t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
			return msg, nil
		}
		currMsg["data"] = doc
	}

	if value, err = t.vm.ToValue(currMsg); err != nil {
		t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
		return msg, nil
	}

	// now that we have finished casting our map to a bunch of different types,
	// lets run our transformer on the document
	beforeVM := time.Now().Nanosecond()
	if outDoc, err = t.vm.Call(`module.exports`, nil, value); err != nil {
		t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
		return msg, nil
	}

	if result, err = outDoc.Export(); err != nil {
		t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
		return msg, nil
	}

	afterVM := time.Now().Nanosecond()

	if err = t.toMsg(result, msg); err != nil {
		t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
		return msg, err
	}

	if t.debug {
		then := time.Now().Nanosecond()
		fmt.Printf("document transformed in %dus.  %d to marshal, %d in the vm, %d to unmarshal\n", (then-now)/1000, (beforeVM-now)/1000, (afterVM-beforeVM)/1000, (then-afterVM)/1000)
	}

	return msg, nil
}

func (t *Transformer) toMsg(incoming interface{}, msg *message.Msg) error {

	switch newMsg := incoming.(type) {
	case map[string]interface{}: // we're a proper message.Msg, so copy the data over
		msg.Op = message.OpTypeFromString(newMsg["op"].(string))
		msg.Timestamp = newMsg["ts"].(int64)
		msg.Namespace = newMsg["ns"].(string)

		switch data := newMsg["data"].(type) {
		case otto.Value:
			exported, err := data.Export()
			if err != nil {
				t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
				return nil
			}
			d, err := mejson.Unmarshal(exported.(map[string]interface{}))
			if err != nil {
				t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
				return nil
			}
			msg.Data = map[string]interface{}(d)
		case map[string]interface{}:
			d, err := mejson.Unmarshal(data)
			if err != nil {
				t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
				return nil
			}
			msg.Data = map[string]interface{}(d)
		default:
			msg.Data = data
		}
	case bool: // skip this doc if we're a bool and we're false
		if !newMsg {
			msg.Op = message.Noop
			return nil
		}
	default: // something went wrong
		return fmt.Errorf("returned doc was not a map[string]interface{}")
	}

	return nil
}

func (t *Transformer) transformerError(lvl adaptor.ErrorLevel, err error, msg *message.Msg) error {
	var data interface{}
	if msg != nil {
		data = msg.Data
	}

	if e, ok := err.(*otto.Error); ok {
		return adaptor.NewError(lvl, t.path, fmt.Sprintf("transformer error (%s)", e.String()), data)
	}
	return adaptor.NewError(lvl, t.path, fmt.Sprintf("transformer error (%s)", err.Error()), data)
}

// Config holds config options for a transformer adaptor
type Config struct {
	// file containing transformer javascript
	// must define a module.exports = function(doc) { .....; return doc }
	Filename  string `json:"filename" doc:"the filename containing the javascript transform fn"`
	Namespace string `json:"namespace" doc:"namespace to transform"`

	// verbose output
	Debug bool `json:"debug" doc:"display debug information"` // debug mode
}
