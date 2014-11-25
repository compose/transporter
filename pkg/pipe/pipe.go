// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipe provides types to help manage transporter communication channels as well as
// event types.
package pipe

import (
	"fmt"
	"time"

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
	In              messageChan
	Out             messageChan
	Err             chan error
	Event           chan Event
	Stopped         bool // has the pipe been stopped?
	chStop          chan chan bool
	listening       bool
	metrics         *nodeMetrics
	metricsInterval time.Duration
}

// NewSourcePipe creates a Pipe that has no listening loop, but just emits messages.  Only one SourcePipe should be created for each transporter pipeline and should be attached to the transporter source.
func NewSourcePipe(name string, interval time.Duration) Pipe {
	p := Pipe{
		In:              nil,
		Out:             newMessageChan(),
		Err:             make(chan error),
		Event:           make(chan Event),
		chStop:          make(chan chan bool),
		metricsInterval: interval,
	}
	p.metrics = NewNodeMetrics(name, p.Event, interval)
	return p
}

// NewJoinPipe creates a pipe that with the In channel attached to the given pipe's Out channel.  Multiple Join pipes can be chained together to create a processing pipeline
func NewJoinPipe(p Pipe, name string) Pipe {
	newp := Pipe{
		In:              p.Out,
		Out:             newMessageChan(),
		Err:             p.Err,
		Event:           p.Event,
		chStop:          make(chan chan bool),
		metricsInterval: p.metricsInterval,
	}
	newp.metrics = NewNodeMetrics(p.metrics.path+"/"+name, p.Event, p.metricsInterval)
	return newp
}

// NewSinkPipe creates a pipe that acts as a terminator to a chain of pipes.  The In channel is the previous channel's Out chan, and the SinkPipe's Out channel is nil.
func NewSinkPipe(p Pipe, name string) Pipe {
	newp := Pipe{
		In:              p.Out,
		Out:             nil,
		Err:             p.Err,
		Event:           p.Event,
		chStop:          make(chan chan bool),
		metricsInterval: p.metricsInterval,
	}

	fmt.Printf("in new sink pipe, p is %+v\n", p)
	newp.metrics = NewNodeMetrics(p.metrics.path+"/"+name, p.Event, p.metricsInterval)
	return newp
}

// Listen starts a listening loop that pulls messages from the In chan, applies fn(msg), a `func(message.Msg) error`, and emits them on the Out channel.
// Errors will be emited to the Pipe's Err chan, and will terminate the loop.
// The listening loop can be interupted by calls to Stop().
func (m *Pipe) Listen(fn func(*message.Msg) (*message.Msg, error)) error {
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
			m.metrics.RecordsIn += 1
			outmsg, err := fn(msg)
			if err != nil {
				m.Err <- err
				return err
			}
			if m.Out != nil {
				m.Send(outmsg)
			}
		case <-time.After(100 * time.Millisecond):
			// NOP, just breath
		}
	}
}

// Stop terminates the channels listening loop, and allows any timeouts in send to fail
func (m *Pipe) Stop() {
	if !m.Stopped {
		m.Stopped = true
		m.metrics.Stop()

		// we only worry about the stop channel if we're in a listening loop
		if m.listening {
			c := make(chan bool)
			m.chStop <- c
			<-c
		}
	}
}

// send emits the given message on the 'Out' channel.  the send Timesout after 100 ms in order to chaeck of the Pipe has stopped and we've been asked to exit.
// If the Pipe has been stopped, the send will fail and there is no guarantee of either success or failure
func (m *Pipe) Send(msg *message.Msg) {
	for {
		select {
		case m.Out <- msg:
			m.metrics.RecordsOut += 1
			return
		case <-time.After(100 * time.Millisecond):
			if m.Stopped {
				return
			}
		}
	}
}
