// Copyright 2014 The Transporter Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package pipeline provides all adaptoremented functionality to move
// data through transporter.
package pipeline

import (
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

var (
	// ErrResumeTimedOut is returned when the resumeTimeout is reached after attempting
	// to check that a sink offset matches the newest offset.
	ErrResumeTimedOut = errors.New("resume timeout reached")
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
	Children   []*Node
	Parent     *Node
	Transforms []*Transform

	nsFilter      *regexp.Regexp
	c             client.Client
	reader        client.Reader
	writer        client.Writer
	done          chan struct{}
	wg            sync.WaitGroup
	l             log.Logger
	pipe          *pipe.Pipe
	clog          *commitlog.CommitLog
	om            offset.Manager
	resumeTimeout time.Duration
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
		Name:          name,
		Type:          kind,
		nsFilter:      compiledNs,
		pipe:          pipe.NewPipe(nil, ""),
		Children:      make([]*Node, 0),
		Transforms:    make([]*Transform, 0),
		done:          make(chan struct{}),
		c:             &client.Mock{},
		reader:        &client.MockReader{},
		writer:        &client.MockWriter{},
		resumeTimeout: 60 * time.Second,
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
		n.Parent = parent
		// TODO: remove path param
		n.pipe = pipe.NewPipe(parent.pipe, "")
		parent.Children = append(parent.Children, n)
		return nil
	}
}

// WithTransforms adds the provided transforms to be applied in the pipeline.
func WithTransforms(t []*Transform) OptionFunc {
	return func(n *Node) error {
		n.Transforms = t
		return nil
	}
}

// WithCommitLog configures a CommitLog for the reader to persist messages.
func WithCommitLog(dataDir string, maxBytes int) OptionFunc {
	return func(n *Node) error {
		clog, err := commitlog.New(
			commitlog.WithPath(dataDir),
			commitlog.WithMaxSegmentBytes(int64(maxBytes)),
		)
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

// WithOffsetManager configures an offset.Manager to track message offsets.
// func WithOffsetManager(name, dataDir string) OptionFunc {
func WithOffsetManager(om offset.Manager) OptionFunc {
	return func(n *Node) error {
		n.om = om
		return nil
	}
}

func (n *Node) String() string {
	var (
		s, prefix string
		depth     = n.depth()
	)

	prefixformatter := fmt.Sprintf("%%%ds%%-%ds", depth, 18-depth)

	if n.Parent == nil { // root node
		prefix = fmt.Sprintf(prefixformatter, " ", "- Source: ")
	} else {
		prefix = fmt.Sprintf(prefixformatter, " ", "- Sink: ")
	}

	s += fmt.Sprintf("%s %-40s %-15s %-30s", prefix, n.Name, n.Type, n.nsFilter.String())

	for _, child := range n.Children {
		s += "\n" + child.String()
	}
	return s
}

// depth is a measure of how deep into the node tree this node is.  Used to indent the String() stuff
func (n *Node) depth() int {
	if n.Parent == nil {
		return 1
	}

	return 1 + n.Parent.depth()
}

// Path returns a string representation of the names of all the node's parents concatenated with "/"  used in metrics
// eg. for the following tree
// source := pipeline.NewNodeWithOptions("name1", "mongo", "/.*/")
// sink1 := pipeline.NewNodeWithOptions("sink1", "file", "/.*/",
//   pipeline.WithParent(source),
// )
// 'source' will have a Path of 'name1', and 'sink1' will have a path of 'name1/sink1'
func (n *Node) Path() string {
	if n.Parent == nil {
		return n.Name
	}

	return n.Parent.Path() + "/" + n.Name
}

// Start starts the nodes children in a go routine, and then runs either Start() or Listen()
// on the node's adaptor.  Root nodes (nodes with no parent) will run Start()
// and will emit messages to it's children,
// All descendant nodes run Listen() on the adaptor
func (n *Node) Start() error {
	path := n.Path()
	n.l = log.With("name", n.Name).With("type", n.Type).With("path", path)

	for _, child := range n.Children {
		child.l = log.With("name", child.Name).With("type", child.Type).With("path", child.Path())
		go func(node *Node) {
			if err := node.Start(); err != nil {
				node.l.Errorln(err)
			}
		}(child)
	}

	if n.Parent == nil {
		msgMap := make(map[string]client.MessageSet)
		nsOffsetMap := make(map[string]uint64)
		errc := make(chan error, len(n.Children))
		// TODO: not entirely sure about this logic check...
		if n.clog.OldestOffset() != n.clog.NewestOffset() {
			var wg sync.WaitGroup
			n.l.With("newestOffset", n.clog.NewestOffset()).
				With("oldestOffset", n.clog.OldestOffset()).
				Infoln("existing messages in commitlog, checking writer offsets...")
			for _, child := range n.Children {
				n.l.With("name", child.Name).Infof("offsetMap: %+v", child.om.OffsetMap())
				// we subtract 1 from NewestOffset() because we only need to catch up
				// to the last entry in the log
				if child.om.NewestOffset() < (n.clog.NewestOffset() - 1) {
					r, err := n.clog.NewReader(child.om.NewestOffset())
					if err != nil {
						return err
					}
					wg.Add(1)
					go func(r io.Reader) {
						errc <- child.resume(n.clog.NewestOffset()-1, r)
						wg.Done()
					}(r)
				}

				// compute a map of the oldest offset for every namespace from each child
				for ns, offset := range child.om.OffsetMap() {
					if currentOffset, ok := nsOffsetMap[ns]; !ok || currentOffset > offset {
						nsOffsetMap[ns] = offset
					}
				}
			}
			go func() {
				n.l.Infoln("waiting for all children to resume...")
				wg.Wait()
				n.l.Infoln("done waiting for all children to resume")
				close(errc)
			}()
			for err := range errc {
				if err != nil {
					n.l.Errorln(err)
					return err
				}
			}
			n.l.Infoln("done checking for resume errors")
			for ns, offset := range nsOffsetMap {
				r, err := n.clog.NewReader(int64(offset))
				if err != nil {
					return err
				}
				_, size, ts, mode, op, err := commitlog.ReadHeader(r)
				if err != nil {
					return err
				}
				_, val, err := commitlog.ReadKeyValue(size, r)
				if err != nil {
					return err
				}
				d := make(map[string]interface{})
				if err := json.Unmarshal(val, &d); err != nil {
					return err
				}
				data, err := mejson.Unmarshal(d)
				if err != nil {
					return err
				}
				msg := message.From(op, ns, map[string]interface{}(data))
				msgMap[ns] = client.MessageSet{
					Msg:       msg,
					Timestamp: int64(ts),
					Mode:      mode,
				}
			}
		}
		n.l.Infof("starting with metadata %+v", msgMap)
		return n.start(msgMap)
	}

	return n.listen()
}

func (n *Node) resume(newestOffset int64, r io.Reader) error {
	n.l.Infoln("adaptor Resuming...")
	defer func() {
		n.l.Infoln("adaptor Resume complete")
	}()

	for {
		// read each message one at a time by getting the size and then
		// the message and send down the pipe
		logOffset, size, _, _, op, err := commitlog.ReadHeader(r)
		if err != nil {
			return err
		}
		ns, val, err := commitlog.ReadKeyValue(size, r)
		if err != nil {
			return err
		}
		d := make(map[string]interface{})
		if err := json.Unmarshal(val, &d); err != nil {
			return err
		}
		data, err := mejson.Unmarshal(d)
		if err != nil {
			return err
		}
		// if (offset % 1000) == 0 {
		// 	percentComplete := (float64(offset) / float64(newestOffset)) * 100.0
		// 	n.l.With("offset", offset).With("log_offset", newestOffset).With("percent_complete", percentComplete).Infoln("still resuming...")
		// }
		n.pipe.In <- message.From(op, string(ns), map[string]interface{}(data))
		// TODO: remove this when https://github.com/compose/transporter/issues/327
		// is implemented
		n.om.CommitOffset(offset.Offset{
			Namespace: string(ns),
			LogOffset: logOffset,
			Timestamp: time.Now().Unix(),
		})
		if logOffset == uint64(newestOffset) {
			n.l.Infoln("offset of message sent down pipe matches newestOffset")
			break
		}
	}

	n.l.Infoln("all messages sent down pipeline, waiting for offsets to match...")
	timeout := time.After(n.resumeTimeout)
	for {
		select {
		case <-timeout:
			return ErrResumeTimedOut
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

// Start the adaptor as a source
func (n *Node) start(nsMap map[string]client.MessageSet) error {
	n.l.Infoln("adaptor Starting...")

	s, err := n.c.Connect()
	if err != nil {
		return err
	}
	if closer, ok := s.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing session...")
			closer.Close()
			n.l.Infoln("session closed...")
		}()
	}
	readFunc := n.reader.Read(nsMap, func(check string) bool { return n.nsFilter.MatchString(check) })
	msgChan, err := readFunc(s, n.done)
	if err != nil {
		return err
	}
	for msg := range msgChan {
		d, _ := mejson.Marshal(msg.Msg.Data().AsMap())
		b, _ := json.Marshal(d)
		logOffset, err := n.clog.Append(
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
		// TODO: remove this when https://github.com/compose/transporter/issues/327
		// is implemented
		for _, child := range n.Children {
			child.om.CommitOffset(offset.Offset{
				Namespace: msg.Msg.Namespace(),
				LogOffset: uint64(logOffset),
				Timestamp: time.Now().Unix(),
			})
		}
		n.l.With("offset", logOffset).Debugln("attaching offset to message")
		n.pipe.Send(msg.Msg)
	}

	n.l.Infoln("adaptor Start finished...")
	return nil
}

func (n *Node) listen() (err error) {
	n.l.Infoln("adaptor Listening...")
	defer n.l.Infoln("adaptor Listen closed...")

	// TODO: keep n.nsFilter here and remove from pipe.Pipe, we can filter
	// out messages by namespace below in write(), this will allow us to keep
	// the offsetmanager.Manager contained within here and not need to provide
	// it to pipe.Pipe for the cases where we need to ack messages that get
	// filtered out by the namespace filter
	return n.pipe.Listen(n.write, n.nsFilter)
}

func (n *Node) write(msg message.Msg) (message.Msg, error) {
	// TODO: defer func to check if there was an error and if not,
	// call n.om.CommitOffset()
	transformedMsg, err := n.applyTransforms(msg)
	if err != nil {
		return msg, err
	} else if transformedMsg == nil {
		return nil, nil
	}
	sess, err := n.c.Connect()
	if err != nil {
		return msg, err
	}
	defer func() {
		if s, ok := sess.(client.Closer); ok {
			s.Close()
		}
	}()
	returnMsg, err := n.writer.Write(transformedMsg)(sess)
	if err != nil {
		n.pipe.Err <- adaptor.Error{
			Lvl:    adaptor.ERROR,
			Path:   n.Path(),
			Err:    fmt.Sprintf("write message error (%s)", err),
			Record: msg.Data,
		}
	}
	return returnMsg, err
}

func (n *Node) applyTransforms(msg message.Msg) (message.Msg, error) {
	if msg.OP() != ops.Command {
		for _, transform := range n.Transforms {
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
	for _, node := range n.Children {
		node.Stop()
	}
}

func (n *Node) stop() error {
	n.l.Infoln("adaptor Stopping...")
	n.pipe.Stop()

	close(n.done)
	n.wg.Wait()

	if closer, ok := n.writer.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing writer...")
			closer.Close()
			n.l.Infoln("writer closed...")
		}()
	}
	if closer, ok := n.c.(client.Closer); ok {
		defer func() {
			n.l.Infoln("closing connection...")
			closer.Close()
			n.l.Infoln("connection closed...")
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
	if n.Parent == nil && len(n.Children) == 0 { // the root node should have children
		return false
	}

	for _, child := range n.Children {
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
	for _, child := range n.Children {
		childMap := child.Endpoints()
		for k, v := range childMap {
			m[k] = v
		}
	}
	return m
}
