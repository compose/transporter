package file

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/compose/transporter/adaptor"
)

func TestRead(t *testing.T) {
	a, err := adaptor.GetAdaptor(
		"file",
		map[string]interface{}{"uri": fmt.Sprintf("file://%s", filepath.Join("testdata", "start_test.json"))},
	)
	if err != nil {
		t.Fatalf("unexpected GetV2() error, %s", err)
	}
	c, err := a.Client()
	if err != nil {
		t.Errorf("unexpected Client() error, %s", err)
	}
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unexpected Connect() error, %s", err)
	}
	r, err := a.Reader()
	if err != nil {
		t.Fatalf("unexpected Reader() error, %s", err)
	}
	readFunc := r.Read(func(string) bool { return true })
	done := make(chan struct{})
	defer close(done)
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read() error, %s", err)
	}
	var count int
	for range msgChan {
		count++
	}
	if count != 10 {
		t.Errorf("unexpected message count, expected %d, got %d\n", 10, 10)
	}
}
