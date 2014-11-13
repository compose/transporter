package pipe

import (
	"time"

	"github.com/compose/transporter/pkg/message"
)

/*
 * TODO:
 * it's probably entirely reasonable to make the 'Pipe' functionality part of the Node struct.
 * each nodeImpl will need to remember it's parent node, and instead of 'NewPipe' and 'JoinPipe', we would
 * have something slightly different
 *
 * or maybe not.. transformers need pipes too, and they aren't nodes.  what to do
 */

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
	In        messageChan
	Out       messageChan
	Err       chan error
	Event     chan event
	chStop    chan chan bool
	listening bool
	stopped   bool
	metrics   *nodeMetrics
}

/*
 * NewPipe should be called once for each transporter pipeline and will be attached to the transporter source.
 * it initializes all the channels
 */
func NewPipe(name string, interval time.Duration) Pipe {
	p := Pipe{
		In:     nil,
		Out:    newMessageChan(),
		Err:    make(chan error),
		Event:  make(chan event),
		chStop: make(chan chan bool),
	}
	p.metrics = NewNodeMetrics(name, p.Event, interval)
	return p
}

/*
 * JoinPipe should be called to create a pipe that is connected to a previous pipe.
 * the newly created pipe will use the original pipe's 'Out' channel as it's 'In' channel
 * and allows the easy creation of chains of pipes
 */
func JoinPipe(p Pipe, name string, interval time.Duration) Pipe {
	newp := Pipe{
		In:     p.Out,
		Out:    newMessageChan(),
		Err:    p.Err,
		Event:  p.Event,
		chStop: make(chan chan bool),
	}
	newp.metrics = NewNodeMetrics(p.metrics.path+"/"+name, p.Event, interval)
	return newp
}

func TerminalPipe(p Pipe, name string, interval time.Duration) Pipe {
	newp := Pipe{
		In:     p.Out,
		Out:    nil,
		Err:    p.Err,
		Event:  p.Event,
		chStop: make(chan chan bool),
	}
	newp.metrics = NewNodeMetrics(p.metrics.path+"/"+name, p.Event, interval)
	return newp
}

/*
 * start a listening loop.  monitors the chStop for stop events.
 */
func (m *Pipe) Listen(fn func(*message.Msg) error) error {
	if m.In == nil {
		return nil
	}
	m.listening = true
	defer func() {
		m.stopped = true
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
			if m.Out != nil {
				m.Send(msg)
			}
		case <-time.After(1 * time.Second):
			// NOP, just breath
		}
	}
}

func (m *Pipe) Stop() {
	if !m.stopped {
		m.stopped = true
		m.metrics.Stop()

		// we only worry about the stop channel if we're in a listening loop
		if m.listening {
			c := make(chan bool)
			m.chStop <- c
			<-c
		}
	}
}

func (m *Pipe) Stopped() bool {
	return m.stopped
}

/*
 * send the message on the 'Out' channel.  Timeout after 1 second and check if we've been asked to exit.
 * this does not return any information about whether or not the message was sent successfully.  errors should be caught elsewhere
 */
func (m *Pipe) Send(msg *message.Msg) {
	for {
		select {
		case m.Out <- msg:
			m.metrics.RecordsOut += 1
			return
		case <-time.After(1 * time.Second):
			if m.Stopped() {
				return
			}
		}
	}
}
