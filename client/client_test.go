package client_test

import (
	"reflect"
	"sync"
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

var (
	writeErrorTests = []struct {
		c   client.Client
		w   client.Writer
		err error
	}{
		{&client.MockErr{}, &client.MockWriter{}, client.ErrMockConnect},
		{&client.Mock{}, &client.MockErrWriter{}, client.ErrMockWrite},
	}
)

func TestWriteWithError(t *testing.T) {
	for _, wt := range writeErrorTests {
		_, err := client.Write(wt.c, wt.w, message.From(ops.Insert, "test", map[string]interface{}{"hello": "client"}))
		if !reflect.DeepEqual(err, wt.err) {
			t.Errorf("wrong Write() error, expected %s, got %s", wt.err, err)
		}
	}
}

func TestWriteWithConfirms(t *testing.T) {
	w := &client.MockWriter{}
	c := &client.Mock{}
	defer c.Close()
	confirms := make(chan struct{})
	var writeConfirmed bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		<-confirms
		writeConfirmed = true
		wg.Done()
	}(&wg)
	msg := message.From(ops.Insert, "test", map[string]interface{}{"hello": "client"})
	msg = message.WithConfirms(confirms, msg)
	_, err := client.Write(c, w, msg)
	if err != nil {
		t.Fatalf("unexpected Write error, %s", err)
	}
	if w.MsgCount != 1 {
		t.Errorf("message never received")
	}
	wg.Wait()
	if !writeConfirmed {
		t.Errorf("write was not confirmed but should have been")
	}
}

var (
	testMsgCount = 10
)

func TestRead(t *testing.T) {
	c := client.Mock{}
	s, _ := c.Connect()
	r := client.MockReader{MsgCount: testMsgCount}
	readFunc := r.Read(map[string]client.MessageSet{}, func(string) bool { return true })
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
