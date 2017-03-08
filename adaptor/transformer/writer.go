package transformer

import (
	"fmt"
	"time"

	"github.com/compose/mejson"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
	"github.com/robertkrimen/otto"
)

var (
	_ client.Writer = &Writer{}
)

// Writer implements the client.Writer interface.
type Writer struct{}

func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		// short circuit for deletes and commands
		if msg.OP() == ops.Command {
			return msg, nil
		}

		return w.transformOne(s.(*Session).vm, msg)
	}
}

func (w *Writer) transformOne(vm *otto.Otto, msg message.Msg) (message.Msg, error) {
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

	if value, err = vm.ToValue(currMsg); err != nil {
		// t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
		return msg, err
	}

	// now that we have finished casting our map to a bunch of different types,
	// lets run our transformer on the document
	beforeVM := time.Now().Nanosecond()
	if outDoc, err = vm.Call(`module.exports`, nil, value); err != nil {
		// t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
		return msg, err
	}

	if result, err = outDoc.Export(); err != nil {
		// t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
		return msg, err
	}
	afterVM := time.Now().Nanosecond()
	newMsg, err := toMsg(msg, result)
	if err != nil {
		// t.pipe.Err <- t.transformerError(adaptor.ERROR, err, msg)
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
