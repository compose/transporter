package main

import (
	"bytes"
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
	"github.com/compose/transporter/events"
	"github.com/compose/transporter/function"
	"github.com/compose/transporter/pipeline"
	"github.com/dop251/goja"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/oklog/oklog/pkg/group"
)

const (
	DefaultNamespace = "/.*/"
)

func NewBuilder(file string) (*Transporter, error) {
	t := &Transporter{}
	t.vm = goja.New()
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

type Transporter struct {
	vm *goja.Runtime

	sourceNode *pipeline.Node
}

type Node struct {
	vm     *goja.Runtime
	parent *pipeline.Node
}

type Transformer struct {
	vm         *goja.Runtime
	source     *pipeline.Node
	transforms []*pipeline.Transform
}

type Adaptor struct {
	name string
	a    adaptor.Adaptor
}

func (t *Transporter) Run() error {
	var g group.Group
	p, err := pipeline.NewPipeline(version, t.sourceNode, events.LogEmitter(), 60*time.Second)
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

func (t *Transporter) Source(call goja.FunctionCall) goja.Value {
	name, out, namespace := exportArgs(call.Arguments)
	a := out.(Adaptor)
	n, err := pipeline.NewNode(name, a.name, namespace, a.a, nil)
	if err != nil {
		panic(err)
	}
	t.sourceNode = n
	return t.vm.ToValue(&Node{t.vm, n})
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
	child, err := pipeline.NewNode(name, a.name, namespace, a.a, n.parent)
	if err != nil {
		panic(err)
	}
	return n.vm.ToValue(&Node{n.vm, child})
}

func (tf *Transformer) Save(call goja.FunctionCall) goja.Value {
	name, out, namespace := exportArgs(call.Arguments)
	a := out.(Adaptor)
	child, err := pipeline.NewNode(name, a.name, namespace, a.a, tf.source)
	if err != nil {
		panic(err)
	}
	child.Transforms = tf.transforms
	return tf.vm.ToValue(&Node{tf.vm, child})
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
		namespace = DefaultNamespace
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
