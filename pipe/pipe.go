// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipe provides types to help manage transporter communication channels as well as
// event types.
package pipe

import (
	"errors"
	"sync"
	"time"

	"github.com/compose/transporter/events"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/offset"
)

var (
	// ErrUnableToListen is returned in cases where Listen is called before the In chan has been
	// established.
	ErrUnableToListen = errors.New("Listen called without a nil In chan")
)

type messageChan chan TrackedMessage

func newMessageChan() messageChan {
	return make(chan TrackedMessage, 10)
}

type TrackedMessage struct {
	Msg message.Msg
	Off offset.Offset
}

// Pipe provides a set of methods to let transporter nodes communicate with each other.
//
// Pipes contain In, Out, Err, and Event channels. Messages are consumed by a node through the 'in' chan, emitted from the node by the 'out' chan.
// Pipes come in three flavours, a sourcePipe, which only emits messages and has no listening loop, a sinkPipe which has a listening loop, but doesn't emit any messages,
// and joinPipe which has a li tening loop that also emits messages.
type Pipe struct {
	In      messageChan
	Out     []messageChan
	Err     chan error
	Event   chan events.Event
	Stopped bool // has the pipe been stopped?

	MessageCount int

	path      string // the path of this pipe (for events and errors)
	chStop    chan struct{}
	listening bool
	wg        sync.WaitGroup
}

// NewPipe creates a new Pipe.  If the pipe that is passed in is nil, then this pipe will be treated as a source pipe that just serves to emit messages.
// Otherwise, the pipe returned will be created and chained from the last member of the Out slice of the parent.  This function has side effects, and will add
// an Out channel to the pipe that is passed in
func NewPipe(pipe *Pipe, path string) *Pipe {

	p := &Pipe{
		Out:    make([]messageChan, 0),
		path:   path,
		chStop: make(chan struct{}),
	}

	if pipe != nil {
		pipe.Out = append(pipe.Out, newMessageChan())
		p.In = pipe.Out[len(pipe.Out)-1] // use the last out channel
		p.Err = pipe.Err
		p.Event = pipe.Event
	} else {
		p.Err = make(chan error)
		p.Event = make(chan events.Event, 10) // buffer the event channel
	}

	return p
}

// Listen starts a listening loop that pulls messages from the In chan, applies fn(msg), a `func(message.Msg) error`, and emits them on the Out channel.
// Errors will be emitted to the Pipe's Err chan, and will terminate the loop.
// The listening loop can be interrupted by calls to Stop().
func (p *Pipe) Listen(fn func(message.Msg, offset.Offset) (message.Msg, error)) error {
	if p.In == nil {
		return ErrUnableToListen
	}
	p.listening = true
	p.wg.Add(1)
	for {
		select {
		case <-p.chStop:
			if len(p.In) > 0 {
				log.With("path", p.path).With("buffer_length", len(p.In)).Infoln("received stop, message buffer not empty, continuing...")
				continue
			}
			log.Infoln("received stop, message buffer is empty, closing...")
			p.wg.Done()
			return nil
		case m := <-p.In:
			outmsg, err := fn(m.Msg, m.Off)
			if err != nil {
				p.Stopped = true
				p.Err <- err
				p.wg.Done()
				return err
			}
			if outmsg == nil {
				break
			}
			if len(p.Out) > 0 {
				p.Send(outmsg, m.Off)
			} else {
				p.MessageCount++ // update the count anyway
			}
		}
	}
}

// Stop terminates the channels listening loop, and allows any timeouts in send to fail
func (p *Pipe) Stop() {
	if !p.Stopped {
		p.Stopped = true

		// we only worry about the stop channel if we're in a listening loop
		if p.listening {
			close(p.chStop)
			p.wg.Wait()
			return
		}

		timeout := time.After(10 * time.Second)
		for {
			select {
			case <-timeout:
				log.With("path", p.path).Errorln("timeout reached waiting for Out channels to clear")
				return
			default:
			}
			if p.empty() {
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (p *Pipe) empty() bool {
	for _, ch := range p.Out {
		if len(ch) > 0 {
			return false
		}
	}
	return true
}

// Send emits the given message on the 'Out' channel.  the send Timesout after 100 ms in order to chaeck of the Pipe has stopped and we've been asked to exit.
// If the Pipe has been stopped, the send will fail and there is no guarantee of either success or failure
func (p *Pipe) Send(msg message.Msg, off offset.Offset) {
	p.MessageCount++
	for _, ch := range p.Out {

	A:
		for {
			select {
			case ch <- TrackedMessage{msg, off}:
				break A
			}
		}
	}
}
