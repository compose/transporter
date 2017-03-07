package gojajs

import (
	"errors"
	"time"

	"github.com/compose/mejson"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	"github.com/dop251/goja"
)

var (
	_ client.Writer = &Writer{}

	// ErrInvalidMessageType is a generic error returned when the `data` property returned in the document from
	// the JS function was not of type map[string]interface{}
	ErrInvalidMessageType = errors.New("returned document was not a map")
)

// Writer implements the client.Writer interface.
type Writer struct{}

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		// short circuit for commands
		if msg.OP() == ops.Command {
			return msg, nil
		}

		return w.transformOne(s.(*Session), msg)
	}
}

func (w *Writer) transformOne(s *Session, msg message.Msg) (message.Msg, error) {
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
	outDoc = s.fn(currMsg)

	var res map[string]interface{}
	if s.vm.ExportTo(outDoc, &res); err != nil {
		return msg, err
	}
	afterVM := time.Now().Nanosecond()
	newMsg, err := toMsg(s.vm, msg, res)
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
