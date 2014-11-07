package node

import (
	"time"

	"github.com/MongoHQ/transporter/pkg/message"
)

type messageChan chan *message.Msg

func newMessageChan() messageChan {
	return make(chan *message.Msg)
}

/*
 * wrap all our messaging methods
 * provide easy way to send and recieve messages while taking into account selects
 * from the stop channel
 */
type Pipe struct {
	In      messageChan
	Out     messageChan
	Err     chan error
	Event   chan Event
	chStop  chan chan bool
	running bool
	metrics *NodeMetrics
}

func NewPipe(name string) Pipe {
	p := Pipe{
		In:     newMessageChan(),
		Out:    newMessageChan(),
		Err:    make(chan error),
		Event:  make(chan Event),
		chStop: make(chan chan bool),
	}
	p.metrics = NewNodeMetrics(name, p.Event)
	return p
}

func JoinPipe(p Pipe, name string) Pipe {
	newp := Pipe{
		In:     p.Out,
		Out:    newMessageChan(),
		Err:    p.Err,
		Event:  p.Event,
		chStop: make(chan chan bool),
	}
	newp.metrics = NewNodeMetrics(p.metrics.path+"/"+name, p.Event)
	return newp
}

func (m *Pipe) Listen(fn func(*message.Msg) error) error {
	m.running = true
	defer func() {
		m.running = false
	}()
	for {
		// check for stop
		select {
		case c := <-m.chStop:
			c <- true
			return nil
		default:
		}

		select {
		case msg := <-m.In:
			m.metrics.RecordsIn += 1
			err := fn(msg)
			if err != nil {
				m.Err <- err
				return err
			}

		case <-time.After(1 * time.Second):
			// NOP, just breath
		}
	}
}

func (m *Pipe) Stop() {
	m.running = false
	c := make(chan bool)
	m.chStop <- c
	<-c
}

func (m *Pipe) Stopping() bool {
	select {
	case c := <-m.chStop:
		c <- true
		return true
	default:
		return false
	}
}

func (m *Pipe) Running() bool {
	return m.running
}

func (m *Pipe) Send(msg *message.Msg) {
	for {
		select {
		case m.Out <- msg:
			m.metrics.RecordsOut += 1
			return
		case <-time.After(1 * time.Second):
			if m.Stopping() {
				return
			}
		}
	}
}
