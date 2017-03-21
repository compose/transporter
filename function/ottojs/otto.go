package ottojs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/compose/mejson"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	"github.com/robertkrimen/otto"

	_ "github.com/robertkrimen/otto/underscore" // enable underscore
)

var (
	_ function.Function = &Otto{}
	// ErrEmptyFilename will be returned when the profided filename is empty.
	ErrEmptyFilename = errors.New("no filename specified")
)

func init() {
	function.Add(
		"otto",
		func() function.Function {
			return &Otto{}
		},
	)

	// adding for backwards compatibility
	function.Add(
		"transformer",
		func() function.Function {
			return &Otto{}
		},
	)
}

type Otto struct {
	Filename string `json:"filename"`
	vm       *otto.Otto
}

func (o *Otto) Apply(msg message.Msg) (message.Msg, error) {
	if o.vm == nil {
		if err := o.initVM(); err != nil {
			return nil, err
		}
	}
	return o.transformOne(msg)
}

func (o *Otto) initVM() error {
	o.vm = otto.New()

	fn, err := extractFunction(o.Filename)
	if err != nil {
		return err
	}

	// set up the vm environment, make `module = {}`
	if _, err := o.vm.Run(`module = {}`); err != nil {
		return err
	}

	// compile our script
	script, err := o.vm.Compile("", fn)
	if err != nil {
		return err
	}

	// run the script, ignore the output
	_, err = o.vm.Run(script)
	return err
}

func extractFunction(filename string) (string, error) {
	if filename == "" {
		return "", ErrEmptyFilename
	}

	ba, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}

	return string(ba), nil
}

func (o *Otto) transformOne(msg message.Msg) (message.Msg, error) {
	var (
		value, outDoc otto.Value
		result, doc   interface{}
		err           error
	)

	now := time.Now().Nanosecond()
	currMsg := data.Data{
		"ts": msg.Timestamp(),
		"op": msg.OP().String(),
		"ns": msg.Namespace(),
	}

	curData := msg.Data()
	doc, err = mejson.Marshal(curData.AsMap())
	if err != nil {
		return msg, err
	}
	currMsg["data"] = doc

	if value, err = o.vm.ToValue(currMsg); err != nil {
		return msg, err
	}

	// now that we have finished casting our map to a bunch of different types,
	// lets run our transformer on the document
	beforeVM := time.Now().Nanosecond()
	if outDoc, err = o.vm.Call(`module.exports`, nil, value); err != nil {
		return msg, err
	}

	if result, err = outDoc.Export(); err != nil {
		return msg, err
	}
	afterVM := time.Now().Nanosecond()
	newMsg, err := toMsg(msg, result)
	if err != nil {
		return msg, err
	}
	then := time.Now().Nanosecond()
	log.With("transformed_in_micro", (then-now)/1000).
		With("marshaled_in_micro", (beforeVM-now)/1000).
		With("vm_time_in_micro", (afterVM-beforeVM)/1000).
		With("unmarshaled_in_micro", (then-afterVM)/1000).
		Debugln("document transformed")

	return newMsg, nil
}

func toMsg(origMsg message.Msg, incoming interface{}) (message.Msg, error) {
	var (
		op      ops.Op
		ts      = origMsg.Timestamp()
		ns      = origMsg.Namespace()
		mapData = origMsg.Data()
	)
	switch newMsg := incoming.(type) {
	case map[string]interface{}, data.Data: // we're a proper message.Msg, so copy the data over
		m := newMsg.(data.Data)
		op = ops.OpTypeFromString(m.Get("op").(string))
		ts = m.Get("ts").(int64)
		ns = m.Get("ns").(string)
		switch newData := m.Get("data").(type) {
		case otto.Value:
			exported, err := newData.Export()
			if err != nil {
				return nil, err
			}
			d, err := mejson.Unmarshal(exported.(map[string]interface{}))
			if err != nil {
				return nil, err
			}
			mapData = data.Data(d)
		case map[string]interface{}:
			newData, err := resolveValues(newData)
			if err != nil {
				return nil, err
			}
			d, err := mejson.Unmarshal(newData)
			if err != nil {
				return nil, err
			}
			mapData = data.Data(d)
		case data.Data:
			newData, err := resolveValues(newData)
			if err != nil {
				return nil, err
			}
			mapData = newData
		default:
			// this was setting the data directly instead of erroring before, recheck
			return nil, fmt.Errorf("bad type for data: %T", newData)
		}
	case bool: // skip this doc if we're a bool and we're false
		if !newMsg {
			return nil, nil
		}
	default: // something went wrong
		return nil, fmt.Errorf("returned doc was not a map[string]interface{}: was %T", newMsg)
	}
	msg := message.From(op, ns, mapData).(*message.Base)
	msg.TS = ts
	return msg, nil
}

func resolveValues(m data.Data) (data.Data, error) {
	for k, v := range m {
		switch v.(type) {
		case otto.Value:
			val, err := v.(otto.Value).Export()
			if err != nil {
				return nil, err
			}
			m[k] = val
		}
	}
	return m, nil
}
