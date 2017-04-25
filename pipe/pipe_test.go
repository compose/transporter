package pipe

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/compose/transporter/offset"
)

func TestSend(t *testing.T) {
	var msgsProcessed int
	source := NewPipe(nil, "source")
	sink1 := NewPipe(source, "sink1")
	go sink1.Listen(func(msg message.Msg, _ offset.Offset) (message.Msg, error) {
		msgsProcessed++
		return msg, nil
	})
	sink2 := NewPipe(source, "sink2")
	go sink2.Listen(func(msg message.Msg, _ offset.Offset) (message.Msg, error) {
		msgsProcessed++
		return msg, nil
	})
	source.Send(message.From(ops.Insert, "test", map[string]interface{}{}), offset.Offset{})
	source.Send(message.From(ops.Insert, "test", map[string]interface{}{}), offset.Offset{})
	source.Stop()
	sink1.Stop()
	sink2.Stop()
	if msgsProcessed != 4 {
		t.Errorf("unexpected messages processed count, expected 4, got %d", msgsProcessed)
	}
}

func TestStopMessageInFlight(t *testing.T) {
	var msgsProcessed int
	source := NewPipe(nil, "in-flight-source")
	sink1 := NewPipe(source, "in-flight-sink1")
	go sink1.Listen(func(msg message.Msg, _ offset.Offset) (message.Msg, error) {
		time.Sleep(100 * time.Millisecond)
		msgsProcessed++
		return msg, nil
	})
	for i := 0; i < 20; i++ {
		source.Send(message.From(ops.Insert, "test", map[string]interface{}{}), offset.Offset{})
	}
	sink1.Stop()
	source.Stop()
	if msgsProcessed != 20 {
		t.Errorf("unexpected messages processed count, expected 20, got %d", msgsProcessed)
	}

}

func TestChainMessage(t *testing.T) {
	var sink2ReceivedMessage bool
	source := NewPipe(nil, "source")
	sink1 := NewPipe(source, "sink1")
	go sink1.Listen(func(msg message.Msg, _ offset.Offset) (message.Msg, error) {
		return msg, nil
	})
	sink2 := NewPipe(sink1, "sink2")
	go sink2.Listen(func(msg message.Msg, _ offset.Offset) (message.Msg, error) {
		sink2ReceivedMessage = true
		return msg, nil
	})
	source.Send(message.From(ops.Insert, "test", map[string]interface{}{}), offset.Offset{})
	source.Stop()
	sink1.Stop()
	sink2.Stop()
	if !sink2ReceivedMessage {
		t.Errorf("sink2 didn't receive a message but should have")
	}
}

func TestSkipMessage(t *testing.T) {
	var sink2ReceivedMessage bool
	source := NewPipe(nil, "source")
	sink1 := NewPipe(source, "sink1")
	go sink1.Listen(func(msg message.Msg, _ offset.Offset) (message.Msg, error) {
		return nil, nil
	})
	sink2 := NewPipe(sink1, "sink2")
	go sink2.Listen(func(msg message.Msg, _ offset.Offset) (message.Msg, error) {
		sink2ReceivedMessage = true
		return msg, nil
	})
	source.Send(message.From(ops.Insert, "test", map[string]interface{}{}), offset.Offset{})
	source.Stop()
	sink1.Stop()
	sink2.Stop()
	if sink2ReceivedMessage {
		t.Errorf("sink2 received a message but shouldn't have")
	}
}

var errListen = errors.New("listen error")

func TestListenErr(t *testing.T) {
	source := NewPipe(nil, "source")
	sink := NewPipe(source, "sink")
	go sink.Listen(func(msg message.Msg, _ offset.Offset) (message.Msg, error) {
		return nil, errListen
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup, t *testing.T) {
		err := <-source.Err
		if !reflect.DeepEqual(err, errListen) {
			t.Errorf("wrong error received, expected %s, got %s", errListen, err)
		}
		wg.Done()
	}(&wg, t)
	source.Send(message.From(ops.Insert, "test", map[string]interface{}{}), offset.Offset{})
	source.Send(message.From(ops.Insert, "test", map[string]interface{}{}), offset.Offset{})
	wg.Wait()
	source.Stop()
	sink.Stop()
}

func TestListeNilErr(t *testing.T) {
	source := NewPipe(nil, "source")
	err := source.Listen(nil)
	if err != ErrUnableToListen {
		t.Errorf("wrong error returned, expected %s, got %s", ErrUnableToListen, err)
	}
}
