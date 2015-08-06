// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipe provides types to help manage transporter communication channels as well as
// event types.
package pipe

import (
	"regexp"
	"time"

	"github.com/compose/transporter/pkg/events"
	"github.com/compose/transporter/pkg/message"
)

type messageChan chan *message.Msg

func newMessageChan() messageChan {
	return make(chan *message.Msg)
}

// Pipe provides a set of methods to let transporter nodes communicate with each other.
//
// Pipes contain In, Out, Err, and Event channels.  Messages are consumed by a node through the 'in' chan, emited from the node by the 'out' chan.
// Pipes come in three flavours, a sourcePipe, which only emits messages and has no listening loop, a sinkPipe which has a listening loop, but doesn't emit any messages,
// and joinPipe which has a li tening loop that also emits messages.
type Pipe struct {
	In      messageChan
	Out     []messageChan
	Err     chan error
	Event   chan events.Event
	Stopped bool // has the pipe been stopped?

	MessageCount int
	LastMsg      *message.Msg
	ExtraState   map[string]interface{}

	path      string // the path of this pipe (for events and errors)
	chStop    chan chan bool
	listening bool
}

// NewPipe creates a new Pipe.  If the pipe that is passed in is nil, then this pipe will be treaded as a source pipe that just serves to emit messages.
// Otherwise, the pipe returned will be created and chained from the last member of the Out slice of the parent.  This function has side effects, and will add
// an Out channel to the pipe that is passed in
func NewPipe(pipe *Pipe, path string) *Pipe {

	p := &Pipe{
		Out:    make([]messageChan, 0),
		path:   path,
		chStop: make(chan chan bool),
	}

	if pipe != nil {
		pipe.Out = append(pipe.Out, newMessageChan())
		p.In = pipe.Out[len(pipe.Out)-1] // use the last out channel
		p.Err = pipe.Err
		p.Event = pipe.Event
	} else {
		p.Err = make(chan error)
		p.Event = make(chan events.Event)
	}

	return p
}

// Listen starts a listening loop that pulls messages from the In chan, applies fn(msg), a `func(message.Msg) error`, and emits them on the Out channel.
// Errors will be emited to the Pipe's Err chan, and will terminate the loop.
// The listening loop can be interupted by calls to Stop().
func (m *Pipe) Listen(fn func(*message.Msg) (*message.Msg, error), nsFilter *regexp.Regexp) error {
	if m.In == nil {
		return nil
	}
	m.listening = true
	defer func() {
		m.Stopped = true
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
			if match, err := msg.MatchNamespace(nsFilter); !match || err != nil {
				if err != nil {
					m.Err <- err
					return err
				}

			} else {
				outmsg, err := fn(msg)
				if err != nil {
					m.Err <- err
					return err
				}
				if skipMsg(outmsg) {
					break
				}
				if len(m.Out) > 0 {
					m.Send(outmsg)
				} else {
					m.MessageCount++ // update the count anyway
				}
			}
			m.LastMsg = msg
		case <-time.After(100 * time.Millisecond):
			// NOP, just breath
		}
	}
}

// Stop terminates the channels listening loop, and allows any timeouts in send to fail
func (m *Pipe) Stop() {
	if !m.Stopped {
		m.Stopped = true

		// we only worry about the stop channel if we're in a listening loop
		if m.listening {
			c := make(chan bool)
			m.chStop <- c
			<-c
		}
	}
}

// Send emits the given message on the 'Out' channel.  the send Timesout after 100 ms in order to chaeck of the Pipe has stopped and we've been asked to exit.
// If the Pipe has been stopped, the send will fail and there is no guarantee of either success or failure
func (m *Pipe) Send(msg *message.Msg) {
	for _, ch := range m.Out {

	A:
		for {
			select {
			case ch <- msg:
				m.MessageCount++
				m.LastMsg = msg
				break A
			case <-time.After(100 * time.Millisecond):
				if m.Stopped {
					// return, with no guarantee
					return
				}
			}
		}
	}
}

// skipMsg returns true if the message should be skipped and not send on to any listening nodes
func skipMsg(msg *message.Msg) bool {
	return msg == nil || msg.Op == message.Noop
}
