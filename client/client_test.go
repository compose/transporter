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
	_, err := client.Write(c, w, message.From(ops.Insert, "test", map[string]interface{}{"hello": "client"}))
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
	_, err := client.Write(c, w, message.From(ops.Insert, "test", map[string]interface{}{"hello": "client"}))
	if err == nil {
		t.Error("no error returned but expected one")
	}
}

var (
	testMsgCount = 10
)

func TestRead(t *testing.T) {
	c := client.Mock{}
	s, _ := c.Connect()
	r := client.MockReader{MsgCount: testMsgCount}
	readFunc := r.Read(func(string) bool { return true })
	msgChan, err := readFunc(s, nil)
	if err != nil {
		t.Fatalf("unexpected readFunc error, %s", err)
	}
	var count int
	for range msgChan {
		count++
	}
	if count != testMsgCount {
		t.Errorf("wrong message count, expected %d, got %d", testMsgCount, count)
	}
}
