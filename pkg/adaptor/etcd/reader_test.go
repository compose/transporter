package etcd

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
)

var filterFunc = func(key string) bool {
	return true
}

var expectedMsgs = []message.Msg{
	message.From(ops.Insert, ".root", map[string]interface{}{"message": "Welcome"}),
	message.From(ops.Insert, ".messages", map[string]interface{}{"english": "Hello World", "spanish": "Hola world"}),
	message.From(ops.Insert, ".messages_languages", map[string]interface{}{"go": "is awesome", "ruby": "is pretty good", "java": "is lame"}),
	message.From(ops.Insert, ".subkey", map[string]interface{}{"message": "Welcome"}),
	message.From(ops.Insert, ".subkey_messages", map[string]interface{}{"english": "Hello World", "spanish": "Hola world"}),
	message.From(ops.Insert, ".subkey_messages_languages", map[string]interface{}{"go": "is awesome", "ruby": "is pretty good", "java": "is lame"}),
}

func TestRead(t *testing.T) {
	r := newReader("")
	readFunc := r.Read(filterFunc)
	done := make(chan struct{})
	defer close(done)
	msgChan, err := readFunc(defaultSession, done)
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	msgs := make(map[string]message.Msg)
	for msg := range msgChan {
		msgs[msg.Namespace()] = msg
	}
	if len(msgs) != len(expectedMsgs) {
		t.Errorf("wrong count, expected %d, got %d", len(expectedMsgs), len(msgs))
	}

	for _, msg := range expectedMsgs {
		if !reflect.DeepEqual(msg.Data(), msgs[msg.Namespace()].Data()) {
			t.Errorf("wrong message, expected %+v, got %+v", msg.Data(), msgs[msg.Namespace()].Data())
		}
	}
}

var expectedSubKeyMsgs = []message.Msg{
	message.From(ops.Insert, "subkey.root", map[string]interface{}{"message": "Welcome"}),
	message.From(ops.Insert, "subkey._messages", map[string]interface{}{"english": "Hello World", "spanish": "Hola world"}),
	message.From(ops.Insert, "subkey._messages_languages", map[string]interface{}{"go": "is awesome", "ruby": "is pretty good", "java": "is lame"}),
}

func TestReadSubKey(t *testing.T) {
	r := newReader("subkey")
	readFunc := r.Read(filterFunc)
	done := make(chan struct{})
	defer close(done)
	msgChan, err := readFunc(defaultSession, done)
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	msgs := make(map[string]message.Msg)
	for msg := range msgChan {
		msgs[msg.Namespace()] = msg
	}
	if len(msgs) != len(expectedSubKeyMsgs) {
		t.Errorf("wrong count, expected %d, got %d", len(expectedSubKeyMsgs), len(msgs))
	}

	for _, msg := range expectedSubKeyMsgs {
		if !reflect.DeepEqual(msg.Data(), msgs[msg.Namespace()].Data()) {
			t.Errorf("wrong message, expected %+v, got %+v", msg.Data(), msgs[msg.Namespace()].Data())
		}
	}
}
