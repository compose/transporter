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
	chStop  chan chan bool
	running bool
}

func NewPipe() Pipe {
	return Pipe{
		In:     newMessageChan(),
		Out:    newMessageChan(),
		Err:    make(chan error),
		chStop: make(chan chan bool),
	}
}

func JoinPipe(p Pipe) Pipe {
	return Pipe{
		In:     p.Out,
		Out:    newMessageChan(),
		Err:    make(chan error),
		chStop: make(chan chan bool),
	}
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
			return
		case <-time.After(1 * time.Second):
			if m.Stopping() {
				return
			}
		}
	}
}
