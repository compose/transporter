package etcd

import (
	"context"
	"fmt"
	"testing"

	"github.com/compose/transporter/pkg/message"
	"github.com/compose/transporter/pkg/message/ops"
)

var mockWriteRootKey = "x"
var mockInsertData = map[string]interface{}{"hello": "world", "moar": "data"}
var mockUpdateData = map[string]interface{}{"hello": "goodbye", "moar": "data"}
var mockDeleteData = map[string]interface{}{"hello": "goodbye", "moar": "data"}

func TestWrite(t *testing.T) {
	w := newWriter(mockWriteRootKey)
	writeData(w, ops.Insert, mockInsertData, t)
	verifyData(mockInsertData, t)

	writeData(w, ops.Update, mockUpdateData, t)
	verifyData(mockUpdateData, t)

	writeData(w, ops.Delete, mockDeleteData, t)
	resp, err := defaultKeysAPI.Get(context.Background(), fmt.Sprintf("%s/b", mockWriteRootKey), nil)
	if err != nil {
		t.Fatalf("unexpected Get error, %s", err)
	}
	if !resp.Node.Dir {
		t.Errorf("expected a node directory from response but didn't get one, %+v", resp)
	}
}

func writeData(w *Writer, op ops.Op, data map[string]interface{}, t *testing.T) {
	writeFunc := w.Write(message.From(op, "a.b", data))
	err := writeFunc(defaultSession)
	if err != nil {
		t.Fatalf("unexpected writeFunc error, %s", err)
	}
}

func verifyData(data map[string]interface{}, t *testing.T) {
	for k, v := range data {
		key := fmt.Sprintf("%s/b/%s", mockWriteRootKey, k)
		resp, err := defaultKeysAPI.Get(context.Background(), key, nil)
		if err != nil {
			t.Fatalf("unexpected Get error, %s", err)
		}
		if resp.Node.Value != v {
			t.Errorf("wrong data after Write, expected %s, got %s", v, resp.Node.Value)
		}
	}
}
