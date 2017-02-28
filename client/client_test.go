package client_test

import (
	"testing"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

func TestWrite(t *testing.T) {
	w := &client.MockWriter{}
	c := &client.Mock{}
	defer c.Close()
	err := client.Write(c, w, message.From(ops.Insert, "test", map[string]interface{}{"hello": "client"}))
	if err != nil {
		t.Fatalf("unexpected Write error, %s", err)
	}
	if w.MsgCount != 1 {
		t.Errorf("message never received")
	}
}

func TestWriteWithError(t *testing.T) {
	w := &client.MockWriter{}
	c := &client.MockErr{}
	err := client.Write(c, w, message.From(ops.Insert, "test", map[string]interface{}{"hello": "client"}))
	if err == nil {
		t.Error("no error returned but expected one")
	}
}
