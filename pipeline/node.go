// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipeline provides all adaptoremented functionality to move
// data through transporter.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/compose/mejson"
	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	"github.com/compose/transporter/offset"
	"github.com/compose/transporter/pipe"
)

const (
	defaultCompactionInterval = 1 * time.Hour
	defaultWriteTimeout       = 5 * time.Second
)

var (
	// ErrResumeTimedOut is returned when the resumeTimeout is reached after attempting
	// to check that a sink offset matches the newest offset.
	ErrResumeTimedOut = errors.New("resume timeout reached")

	// ErrResumeStopped is returned when the underling pipe.Pipe has been stopped while
	// a Node is in the process of resuming.
	ErrResumeStopped = errors.New("pipe has been stopped, canceling resume")

	// ErrConfirmOffset is returned if the underling OffsetManager fails to commit
	// the offsets.
	ErrConfirmOffset = errors.New("failed to confirm offsets")
)

// OptionFunc is a function that configures a Node.
// It is used in NewNodeWithOptions.
type OptionFunc func(*Node) error

// A Node is the basic building blocks of transporter pipelines.
// Nodes are constructed in a tree, with the first node broadcasting
// data to each of it's children.
type Node struct {
	Name       string
	Type       string
	path       string
	depth      int
	children   []*Node
	parent     *Node
	transforms []*Transform

	nsFilter       *regexp.Regexp
	c              client.Client
	reader         client.Reader
	writer         client.Writer
	done           chan struct{}
	wg             sync.WaitGroup
	l              log.Logger
	pipe           *pipe.Pipe
	clog           *commitlog.CommitLog
	om             offset.Manager
	confirms       chan struct{}
	confirmsDone   chan struct{}
	pendingOffsets []offset.Offset
	offsetLock     sync.Mutex
	resumeTimeout  time.Duration
	writeTimeout   time.Duration

	compactionInterval time.Duration
}

// Transform defines the struct for including a native function in the pipeline.
type Transform struct {
	Name     string
	Fn       function.Function
	NsFilter *regexp.Regexp
}

// NewNodeWithOptions initializes a Node with the required parameters and then applies
// each OptionFunc provided.
func NewNodeWithOptions(name, kind, ns string, options ...OptionFunc) (*Node, error) {
	compiledNs, err := regexp.Compile(strings.Trim(ns, "/"))
	if err != nil {
		return nil, err
	}
	n := &Node{
		Name:               name,
		Type:               kind,
		path:               name,
		depth:              1,
		nsFilter:           compiledNs,
		pipe:               pipe.NewPipe(nil, name),
		children:           make([]*Node, 0),
		transforms:         make([]*Transform, 0),
		done:               make(chan struct{}),
		c:                  &client.Mock{},
		reader:             &client.MockReader{},
		writer:             &client.MockWriter{},
		resumeTimeout:      60 * time.Second,
		writeTimeout:       defaultWriteTimeout,
		compactionInterval: defaultCompactionInterval,
	}
	// Run the options on it
	for _, option := range options {
		if err := option(n); err != nil {
			return nil, err
		}
	}
	return n, nil
}

// WithClient sets the client.Client to be used for providing a client.Session to the
// client.Reader/Writer..
func WithClient(a adaptor.Adaptor) OptionFunc {
	return func(n *Node) error {
		cli, err := a.Client()
		n.c = cli
		return err
	}
}

// WithReader sets the client.Reader to be used to source data from.
func WithReader(a adaptor.Adaptor) OptionFunc {
	return func(n *Node) error {
		r, err := a.Reader()
		n.reader = r
		return err
	}
}

// WithWriter sets the client.Writer to be used to send data to.
func WithWriter(a adaptor.Adaptor) OptionFunc {
	return func(n *Node) error {
		w, err := a.Writer(n.done, &n.wg)
		n.writer = w
		return err
	}
}

// WithParent sets the parent node and reconfigures the pipe.
func WithParent(parent *Node) OptionFunc {
	return func(n *Node) error {
		n.parent = parent
		parent.children = append(parent.children, n)
		n.path = parent.path + "/" + n.Name
		n.depth = parent.depth + 1
		n.pipe = pipe.NewPipe(parent.pipe, n.path)
		return nil
	}
}

// WithTransforms adds the provided transforms to be applied in the pipeline.
func WithTransforms(t []*Transform) OptionFunc {
	return func(n *Node) error {
		n.transforms = t
		return nil
	}
}

// WithCommitLog configures a CommitLog for the reader to persist messages.
func WithCommitLog(options ...commitlog.OptionFunc) OptionFunc {
	return func(n *Node) error {
		clog, err := commitlog.New(options...)
		n.clog = clog
		return err
	}
}

// WithResumeTimeout configures how long to wait before all sink offsets match the
// newest offset.
func WithResumeTimeout(timeout time.Duration) OptionFunc {
	return func(n *Node) error {
		n.resumeTimeout = timeout
		return nil
	}
}

// WithWriteTimeout configures the timeout duration for a writer to return.
func WithWriteTimeout(timeout string) OptionFunc {
	return func(n *Node) error {
		if timeout == "" {
			n.writeTimeout = defaultWriteTimeout
			return nil
		}
		wt, err := time.ParseDuration(timeout)
		if err != nil {
			return err
		}
		n.writeTimeout = wt
		return nil
	}
}

// WithOffsetManager configures an offset.Manager to track message offsets.
func WithOffsetManager(om offset.Manager) OptionFunc {
	return func(n *Node) error {
		n.om = om
		return nil
	}
}

// WithCompactionInterval configures the duration for running log compaction.
func WithCompactionInterval(interval string) OptionFunc {
	return func(n *Node) error {
		if interval == "" {
			n.compactionInterval = defaultCompactionInterval
			return nil
		}
		ci, err := time.ParseDuration(interval)
		if err != nil {
			return err
		}
		n.compactionInterval = ci
		return nil
	}
}

func (n *Node) String() string {
	var (
		s, prefix string
	)

	prefixformatter := fmt.Sprintf("%%%ds%%-%ds", n.depth, 18-n.depth)

	if n.parent == nil { // root node
		prefix = fmt.Sprintf(prefixformatter, " ", "- Source: ")
	} else {
		prefix = fmt.Sprintf(prefixformatter, " ", "- Sink: ")
	}

	s += fmt.Sprintf("%s %-40s %-15s %-30s", prefix, n.Name, n.Type, n.nsFilter.String())

	for _, child := range n.children {
		s += "\n" + child.String()
	}
	return s
}

// Start starts the nodes children in a go routine, and then runs either Start() or Listen()
// on the node's adaptor.  Root nodes (nodes with no parent) will run Start()
// and will emit messages to it's children,
// All descendant nodes run Listen() on the adaptor
func (n *Node) Start() error {
	n.l = log.With("name", n.Name).With("type", n.Type).With("path", n.path)

	errors := make(chan error, 1)
	for _, child := range n.children {
		child.l = log.With("name", child.Name).With("type", child.Type).With("path", child.path)
		go func(node *Node) {
			errors <- node.Start()
		}(child)
	}

	if n.parent == nil {
		msgMap := make(map[string]client.MessageSet)
		if n.clog != nil {
			nsOffsetMap := make(map[string]uint64)
			errc := make(chan error, len(n.children))
			// TODO: not entirely sure about this logic check...
			if n.clog.OldestOffset() != n.clog.NewestOffset() {
				n.l.With("newestOffset", n.clog.NewestOffset()).
					With("oldestOffset", n.clog.OldestOffset()).
					Infoln("existing messages in commitlog, checking writer offsets...")
				for _, child := range n.children {
					n.l.With("name", child.Name).Infof("offsetMap: %+v", child.om.OffsetMap())
					// we subtract 1 from NewestOffset() because we only need to catch up
					// to the last entry in the log
					if child.om.NewestOffset() < (n.clog.NewestOffset() - 1) {
						r, err := n.clog.NewReader(child.om.NewestOffset())
						if err != nil {
							return err
						}
						go func(r io.Reader) {
							errc <- child.resume(n.clog.NewestOffset()-1, r)
						}(r)
					} else {
						errc <- nil
					}

				}
				n.l.Infoln("waiting for all children to resume...")
				err := <-errc
				for i := 1; i < cap(errc); i++ {
					e := <-errc
					if err == nil && e != nil {
						err = e
					}
				}
				n.l.Infoln("done waiting for all children to resume")
				if err != nil {
					n.l.Errorln(err)
					return err
				}
				n.l.Infoln("done checking for resume errors")
				// compute a map of the oldest offset for every namespace from each child
				for _, child := range n.children {
					for ns, offset := range child.om.OffsetMap() {
						if currentOffset, ok := nsOffsetMap[ns]; !ok || currentOffset > offset {
							nsOffsetMap[ns] = offset
						}
					}
				}

				for _, offset := range nsOffsetMap {
					r, err := n.clog.NewReader(int64(offset))
					if err != nil {
						return err
					}

					d, err := readResumeData(r)
					if err != nil {
						return err
					}
					mode := d.msg.Mode
					// we overwrite the mode to Complete unless the offset
					// was the last message processed
					if mode == commitlog.Copy && int64(offset) != (n.clog.NewestOffset()-1) {
						mode = commitlog.Complete
					}

					msgMap[d.ns] = client.MessageSet{
						Msg:       d.msg.Msg,
						Timestamp: d.msg.Timestamp,
						Mode:      mode,
					}
				}
			}
			go n.runCompaction()
		}
		n.l.Infof("starting with metadata %+v", msgMap)
		go func() {
			errors <- n.start(msgMap)
		}()
	} else {
		go func() {
			errors <- n.listen()
		}()
	}

	return <-errors
}

func (n *Node) resume(newestOffset int64, r io.Reader) error {
	n.l.Infoln("adaptor Resuming...")
	defer func() {
		n.l.Infoln("adaptor Resume complete")
	}()

	percentComplete := 0.0
	for {
		d, err := readResumeData(r)
		if err != nil {
			return err
		}

		p := (float64(d.offset) / float64(newestOffset)) * 100.0
		if (p - percentComplete) >= 1.0 {
			percentComplete = p
			n.l.With("offset", d.offset).
				With("log_offset", newestOffset).
				With("percent_complete", percentComplete).
				Infoln("still resuming...")
		}
		if n.pipe.Stopped {
			return ErrResumeStopped
		}
		n.pipe.In <- pipe.TrackedMessage{
			Msg: d.msg.Msg,
			Off: offset.Offset{
				Namespace: d.msg.Msg.Namespace(),
				LogOffset: d.offset,
				Timestamp: time.Now().Unix(),
			},
		}
		if d.offset == uint64(newestOffset) {
			n.l.Infoln("offset of message sent down pipe matches newestOffset")
			break
		}
	}

	n.l.With("timeout", n.resumeTimeout).Infoln("all messages sent down pipeline, waiting for offsets to match...")
	timeout := time.After(n.resumeTimeout)
	for {
		select {
		case <-timeout:
			return ErrResumeTimedOut
		case <-n.done:
			return nil
		default:
		}
		n.l.Infoln("checking if offsets match")
		sinkOffset := n.om.NewestOffset()
		if sinkOffset == newestOffset {
			n.l.Infoln("offsets match!!!")
			return nil
		}
		n.l.With("sink_offset", sinkOffset).With("newestOffset", newestOffset).Infoln("offsets did not match, checking again in 5 seconds...")
		time.Sleep(5 * time.Second)
	}
}

func (n *Node) runCompaction() {
	compactor := commitlog.NewNamespaceCompactor(n.clog)
	n.l.With("compaction_interval", n.compactionInterval).Infoln("starting compaction routine")
	ticker := time.NewTicker(n.compactionInterval)
	for {
		select {
		case <-ticker.C:
		case <-n.done:
			n.l.Infoln("stopping compaction routine")
			return
		}
		oldestOffset := uint64(n.clog.NewestOffset())
		for _, child := range n.children {
			if oldestOffset > uint64(child.om.NewestOffset()) {
				oldestOffset = uint64(child.om.NewestOffset())
			}
		}
		compactor.Compact(oldestOffset, n.clog.Segments())
	}
}

// Start the adaptor as a source
func (n *Node) start(nsMap map[string]client.MessageSet) error {
	n.l.Infoln("adaptor Starting...")

	s, err := n.c.Connect()
	if err != nil {
		return err
	}
	if closer, ok := s.(client.Closer); ok {
		defer func() {
			n.l.Debugln("closing session...")
			closer.Close()
			n.l.Debugln("session closed...")
		}()
	}
	readFunc := n.reader.Read(nsMap, func(check string) bool { return n.nsFilter.MatchString(check) })
	msgChan, err := readFunc(s, n.done)
	if err != nil {
		return err
	}
	var logOffset int64
	for msg := range msgChan {
		if n.clog != nil {
			d, _ := mejson.Marshal(msg.Msg.Data().AsMap())
			b, _ := json.Marshal(d)
			o, err := n.clog.Append(
				commitlog.NewLogFromEntry(
					commitlog.LogEntry{
						Key:       []byte(msg.Msg.Namespace()),
						Mode:      msg.Mode,
						Op:        msg.Msg.OP(),
						Timestamp: uint64(msg.Timestamp),
						Value:     b,
					}))
			if err != nil {
				return err
			}
			logOffset = o
			n.l.With("offset", logOffset).Debugln("attaching offset to message")
		}
		n.pipe.Send(msg.Msg, offset.Offset{
			Namespace: msg.Msg.Namespace(),
			LogOffset: uint64(logOffset),
			Timestamp: time.Now().Unix(),
		})
	}

	n.l.Infoln("adaptor Start finished...")
	return nil
}

func (n *Node) listen() error {
	n.l.Infoln("adaptor Listening...")
	defer n.l.Infoln("adaptor Listen closed...")

	errors := make(chan error, 1)
	if n.om != nil {
		errors = make(chan error, 2)
		n.confirms = make(chan struct{})
		n.confirmsDone = make(chan struct{})
		n.pendingOffsets = make([]offset.Offset, 0)
		go func() {
			errors <- n.waitForConfirms()
		}()
	}

	go func() {
		errors <- n.pipe.Listen(n.write)
	}()

	return <-errors
}

type writeResult struct {
	msg message.Msg
	err error
}

func (n *Node) write(msg message.Msg, off offset.Offset) (message.Msg, error) {
	if n.om != nil {
		n.offsetLock.Lock()
		msg = message.WithConfirms(n.confirms, msg)
		n.offsetLock.Unlock()
	}
	if !n.nsFilter.MatchString(msg.Namespace()) {
		n.l.With("ns", msg.Namespace()).Debugln("message skipped by namespace filter")
		if msg.Confirms() != nil {
			n.offsetLock.Lock()
			if len(n.pendingOffsets) == 0 {
				n.om.CommitOffset(off, false)
			}
			n.offsetLock.Unlock()
		}
		return msg, nil
	}
	msg, err := n.applyTransforms(msg)
	if err != nil {
		return nil, err
	} else if msg == nil {
		if n.om != nil {
			n.offsetLock.Lock()
			if len(n.pendingOffsets) == 0 {
				n.om.CommitOffset(off, false)
			}
			n.offsetLock.Unlock()
		}
		return nil, err
	}

	if n.om != nil {
		n.offsetLock.Lock()
		n.pendingOffsets = append(n.pendingOffsets, off)
		n.offsetLock.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.writeTimeout)
	defer cancel()
	c := make(chan writeResult)
	go func() {
		m, err := client.Write(n.c, n.writer, msg)
		c <- writeResult{m, err}
	}()
	select {
	case wr := <-c:
		return wr.msg, wr.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) waitForConfirms() error {
	for {
		select {
		case <-n.confirms:
			if err := n.confirmOffsets(); err != nil {
				return ErrConfirmOffset
			}
		case <-n.confirmsDone:
			n.l.Infoln("waitForConfirms stopped")
			return nil
		}
	}
}

func (n *Node) confirmOffsets() error {
	n.offsetLock.Lock()
	defer n.offsetLock.Unlock()
	for i := len(n.pendingOffsets) - 1; i >= 0; i-- {
		if err := n.om.CommitOffset(n.pendingOffsets[i], false); err != nil {
			return err
		}
	}
	n.pendingOffsets = make([]offset.Offset, 0)
	return nil
}

func (n *Node) applyTransforms(msg message.Msg) (message.Msg, error) {
	if msg.OP() != ops.Command {
		for _, transform := range n.transforms {
			if !transform.NsFilter.MatchString(msg.Namespace()) {
				n.l.With("transform", transform.Name).With("ns", msg.Namespace()).Debugln("filtered message")
				continue
			}
			m, err := transform.Fn.Apply(msg)
			if err != nil {
				n.l.Errorf("transform function error, %s", err)
				return nil, err
			} else if m == nil {
				n.l.With("transform", transform.Name).Debugln("returned nil message, skipping")
				return nil, nil
			}
			msg = m
			if msg.OP() == ops.Skip {
				n.l.With("transform", transform.Name).With("op", msg.OP()).Debugln("skipping message")
				return nil, nil
			}
		}
	}
	return msg, nil
}

// Stop this node's adaptor, and sends a stop to each child of this node
func (n *Node) Stop() {
	n.stop()
	for _, node := range n.children {
		node.Stop()
	}
}

func (n *Node) stop() error {
	n.l.Infoln("adaptor Stopping...")
	n.pipe.Stop()
	close(n.done)
	n.wg.Wait()

	if n.om != nil {
		close(n.confirmsDone)
	}

	if closer, ok := n.writer.(client.Closer); ok {
		defer func() {
			closer.Close()
		}()
	}
	if closer, ok := n.c.(client.Closer); ok {
		defer func() {
			closer.Close()
		}()
	}

	n.l.Infoln("adaptor Stopped")
	return nil
}

// Validate ensures that the node tree conforms to a proper structure.
// Node trees must have at least one source, and one sink.
// dangling transformers are forbidden.  Validate only knows about default adaptors
// in the adaptor package, it can't validate any custom adaptors
func (n *Node) Validate() bool {
	if n.parent == nil && len(n.children) == 0 { // the root node should have children
		return false
	}

	for _, child := range n.children {
		if !child.Validate() {
			return false
		}
	}
	return true
}

// Endpoints recurses down the node tree and accumulates a map associating node name with node type
// this is primarily used with the boot event
func (n *Node) Endpoints() map[string]string {
	m := map[string]string{n.Name: n.Type}
	for _, child := range n.children {
		childMap := child.Endpoints()
		for k, v := range childMap {
			m[k] = v
		}
	}
	return m
}
