package gojajs

import (
	"errors"
	"io/ioutil"
	"time"

	"github.com/compose/mejson"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	"github.com/dop251/goja"
)

var (
	_ function.Function = &Goja{}

	// ErrInvalidMessageType is a generic error returned when the `data` property returned in the document from
	// the JS function was not of type map[string]interface{}
	ErrInvalidMessageType = errors.New("returned document was not a map")

	// ErrEmptyFilename will be returned when the profided filename is empty.
	ErrEmptyFilename = errors.New("no filename specified")
)

func init() {
	function.Add(
		"goja",
		func() function.Function {
			return &Goja{}
		},
	)
	function.Add(
		"js",
		func() function.Function {
			return &Goja{}
		},
	)
}

type Goja struct {
	Filename string `json:"filename"`
	vm       *goja.Runtime
}

// JSFunc defines the structure a transformer function.
type JSFunc func(map[string]interface{}) *goja.Object

// Apply fulfills the function.Function interface by transforming the incoming message with the configured
// JavaScript function.
func (g *Goja) Apply(msg message.Msg) (message.Msg, error) {
	if g.vm == nil {
		if err := g.initVM(); err != nil {
			return nil, err
		}
	}
	return g.transformOne(msg)
}

func (g *Goja) initVM() error {
	g.vm = goja.New()

	fn, err := extractFunction(g.Filename)
	if err != nil {
		return err
	}
	_, err = g.vm.RunString(fn)
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

func (g *Goja) transformOne(msg message.Msg) (message.Msg, error) {
	var (
		outDoc goja.Value
		doc    interface{}
		err    error
	)

	now := time.Now().Nanosecond()
	currMsg := data.Data{"ts": msg.Timestamp(), "op": msg.OP().String(), "ns": msg.Namespace()}

	curData := msg.Data()
	doc, err = mejson.Marshal(curData.AsMap())
	if err != nil {
		return msg, err
	}
	currMsg["data"] = doc

	// lets run our transformer on the document
	beforeVM := time.Now().Nanosecond()
	var jsf JSFunc
	g.vm.ExportTo(g.vm.Get("transform"), &jsf)
	outDoc = jsf(currMsg)

	var res map[string]interface{}
	if g.vm.ExportTo(outDoc, &res); err != nil {
		return msg, err
	}
	afterVM := time.Now().Nanosecond()
	newMsg, err := toMsg(g.vm, msg, res)
	if err != nil {
		return nil, err
	}
	then := time.Now().Nanosecond()
	log.With("transformed_in_micro", (then-now)/1000).
		With("marshaled_in_micro", (beforeVM-now)/1000).
		With("vm_time_in_micro", (afterVM-beforeVM)/1000).
		With("unmarshaled_in_micro", (then-afterVM)/1000).
		Debugln("document transformed")

	return newMsg, nil
}

func toMsg(vm *goja.Runtime, origMsg message.Msg, incoming map[string]interface{}) (message.Msg, error) {
	var (
		op      ops.Op
		ts      = origMsg.Timestamp()
		ns      = origMsg.Namespace()
		mapData = origMsg.Data()
	)
	m := data.Data(incoming)
	op = ops.OpTypeFromString(m.Get("op").(string))
	if op == ops.Skip {
		return nil, nil
	}
	ts = m.Get("ts").(int64)
	ns = m.Get("ns").(string)
	switch newData := m.Get("data").(type) {
	case map[string]interface{}:
		d, err := mejson.Unmarshal(newData)
		if err != nil {
			return nil, err
		}
		mapData = data.Data(d)
	default:
		return nil, ErrInvalidMessageType
	}
	msg := message.From(op, ns, mapData).(*message.Base)
	msg.TS = ts
	return msg, nil
}
