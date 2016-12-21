package gou

import (
	"testing"
	"time"
)

func TestWaitFor(t *testing.T) {

	isDone := false
	foundDone := false
	go func() {
		time.Sleep(time.Second * 1)
		isDone = true
	}()
	WaitFor(func() bool {
		if isDone == true {
			foundDone = true
		}
		return isDone == true
	}, 2)
	if !foundDone {
		t.Fail()
	}
}
