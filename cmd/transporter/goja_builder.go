package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/events"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/offset"
	"github.com/compose/transporter/pipeline"
	"github.com/dop251/goja"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/oklog/run"
)

const (
	defaultNamespace = "/.*/"
)

func newBuilder(file string) (*Transporter, error) {
	t := &Transporter{
		config: &config{},
		vm:     goja.New(),
	}
	t.vm.Set("transporter", t)
	t.vm.Set("t", t.vm.Get("transporter"))
	for _, name := range adaptor.RegisteredAdaptors() {
		t.vm.Set(name, buildAdaptor(name))
	}
	for _, name := range function.RegisteredFunctions() {
		t.vm.Set(name, buildFunction(name))
	}

	ba, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	// configs can have environment variables, replace these before continuing
	ba = setConfigEnvironment(ba)

	if _, err := t.vm.RunString(string(ba)); err != nil {
		return nil, err
	}
	return t, nil
}

// setConfigEnvironment replaces environment variables marked in the form ${FOO} with the
// value stored in the environment variable `FOO`
func setConfigEnvironment(ba []byte) []byte {
	re := regexp.MustCompile(`\$\{([a-zA-Z0-9_]+)\}`)

	matches := re.FindAllSubmatch(ba, -1)
	if matches == nil {
		return ba
	}

	for _, m := range matches {
		v := os.Getenv(string(m[1]))
		ba = bytes.Replace(ba, m[0], []byte(v), -1)
	}

	return ba
}

// Transporter defins the top level construct for creating a pipeline.
type Transporter struct {
	vm *goja.Runtime

	config     *config
	sourceNode *pipeline.Node
}

type config struct {
	LogDir             string `json:"log_dir"`
	MaxSegmentBytes    int    `json:"max_segment_bytes"`
	CompactionInterval string `json:"compaction_interval"`
	WriteTimeout       string `json:"write_timeout"`
}

// Node encapsulates a sink/source node in the pipeline.
type Node struct {
	vm     *goja.Runtime
	parent *pipeline.Node
	config *config
}

// Transformer encapsulates a pipeline.Transform and tracks the Source node.
type Transformer struct {
	vm         *goja.Runtime
	source     *pipeline.Node
	transforms []*pipeline.Transform
	config     *config
}

// Adaptor wraps the underlyig adaptor.Adaptor to be exposed in the JS.
type Adaptor struct {
	name string
	a    adaptor.Adaptor
}

func (t *Transporter) run() error {
	var g run.Group
	p, err := pipeline.NewPipeline(version, t.sourceNode, events.LogEmitter(), 5*time.Second)
	if err != nil {
		return err
	}
	{
		g.Add(func() error {
			return p.Run()
		}, func(error) {
			p.Stop()
		})
	}
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(cancel)
		}, func(error) {
			close(cancel)
		})
	}
	return g.Run()
}

func interrupt(cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-c:
		return fmt.Errorf("received signal %s", sig)
	case <-cancel:
		return errors.New("canceled")
	}
}

// String represents the pipelines as a string
func (t *Transporter) String() string {
	out := "Transporter:\n"
	out += fmt.Sprintf("%s", t.sourceNode.String())
	return out
}

func buildAdaptor(name string) func(map[string]interface{}) Adaptor {
	return func(args map[string]interface{}) Adaptor {
		a, err := adaptor.GetAdaptor(name, args)
		if err != nil {
			panic(err)
		}
		return Adaptor{name, a}
	}
}

func buildFunction(name string) func(map[string]interface{}) function.Function {
	return func(args map[string]interface{}) function.Function {
		f, err := function.GetFunction(name, args)
		if err != nil {
			panic(err)
		}
		return f
	}
}

// Config parses the provided configuration object and associates it with the
// JS VM.
func (t *Transporter) Config(call goja.FunctionCall) goja.Value {
	if cfg, ok := call.Argument(0).Export().(map[string]interface{}); ok {
		b, err := json.Marshal(cfg)
		if err != nil {
			panic(err)
		}

		var c config
		if err = json.Unmarshal(b, &c); err != nil {
			panic(err)
		}
		t.config = &c
	}
	return t.vm.ToValue(t)
}

func (t *Transporter) Source(call goja.FunctionCall) goja.Value {
	name, out, namespace := exportArgs(call.Arguments)
	a := out.(Adaptor)

	options := []pipeline.OptionFunc{
		pipeline.WithClient(a.a),
		pipeline.WithReader(a.a),
		pipeline.WithCompactionInterval(t.config.CompactionInterval),
	}
	if t.config.LogDir != "" {
		options = append(options, pipeline.WithCommitLog(
			[]commitlog.OptionFunc{
				commitlog.WithPath(t.config.LogDir),
				commitlog.WithMaxSegmentBytes(int64(t.config.MaxSegmentBytes)),
			}...))
	}

	n, err := pipeline.NewNodeWithOptions(name, a.name, namespace, options...)
	if err != nil {
		panic(err)
	}
	t.sourceNode = n
	return t.vm.ToValue(&Node{t.vm, n, t.config})
}

func (n *Node) Transform(call goja.FunctionCall) goja.Value {
	name, f, ns := exportArgs(call.Arguments)
	compiledNs, err := regexp.Compile(strings.Trim(ns, "/"))
	if err != nil {
		panic(err)
	}
	tf := &Transformer{
		vm:         n.vm,
		source:     n.parent,
		transforms: make([]*pipeline.Transform, 0),
		config:     n.config,
	}
	tf.transforms = append(tf.transforms, &pipeline.Transform{Name: name, Fn: f.(function.Function), NsFilter: compiledNs})
	return n.vm.ToValue(tf)
}

func (tf *Transformer) Transform(call goja.FunctionCall) goja.Value {
	name, f, ns := exportArgs(call.Arguments)
	compiledNs, err := regexp.Compile(strings.Trim(ns, "/"))
	if err != nil {
		panic(err)
	}
	t := &pipeline.Transform{Name: name, Fn: f.(function.Function), NsFilter: compiledNs}
	tf.transforms = append(tf.transforms, t)
	return tf.vm.ToValue(tf)
}

func (n *Node) Save(call goja.FunctionCall) goja.Value {
	name, out, namespace := exportArgs(call.Arguments)
	a := out.(Adaptor)
	options := []pipeline.OptionFunc{
		pipeline.WithParent(n.parent),
		pipeline.WithClient(a.a),
		pipeline.WithWriter(a.a),
		pipeline.WithWriteTimeout(n.config.WriteTimeout),
	}

	if n.config.LogDir != "" {
		om, err := offset.NewLogManager(n.config.LogDir, name)
		if err != nil {
			panic(err)
		}
		options = append(options, pipeline.WithOffsetManager(om))
	}

	child, err := pipeline.NewNodeWithOptions(name, a.name, namespace, options...)
	if err != nil {
		panic(err)
	}
	return n.vm.ToValue(&Node{n.vm, child, n.config})
}

func (tf *Transformer) Save(call goja.FunctionCall) goja.Value {
	name, out, namespace := exportArgs(call.Arguments)
	a := out.(Adaptor)
	options := []pipeline.OptionFunc{
		pipeline.WithParent(tf.source),
		pipeline.WithClient(a.a),
		pipeline.WithWriter(a.a),
		pipeline.WithTransforms(tf.transforms),
		pipeline.WithWriteTimeout(tf.config.WriteTimeout),
	}

	if tf.config.LogDir != "" {
		om, err := offset.NewLogManager(tf.config.LogDir, name)
		if err != nil {
			panic(err)
		}
		options = append(options, pipeline.WithOffsetManager(om))
	}

	child, err := pipeline.NewNodeWithOptions(name, a.name, namespace, options...)
	if err != nil {
		panic(err)
	}
	return tf.vm.ToValue(&Node{tf.vm, child, tf.config})
}

// arguments can be any of the following forms:
// ("name", Adaptor/Function, "namespace")
// ("name", Adaptor/Function)
// (Adaptor/Function, "namespace")
// (Adaptor/Function)
// the only *required* argument is a Adaptor or Function
func exportArgs(args []goja.Value) (string, interface{}, string) {
	if len(args) == 0 {
		panic("at least 1 argument required")
	}
	uuid, _ := uuid.NewV4()
	var (
		name      = uuid.String()
		namespace = defaultNamespace
		a         interface{}
	)
	if n, ok := args[0].Export().(string); ok {
		name = n
		a = args[1].Export()
		if len(args) == 3 {
			namespace = args[2].Export().(string)
		}
	} else {
		a = args[0].Export()
		if len(args) == 2 {
			namespace = args[1].Export().(string)
		}
	}
	return name, a, namespace
}
