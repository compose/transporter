package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/events"
	"github.com/compose/transporter/pipeline"
	"github.com/dop251/goja"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/oklog/oklog/pkg/group"
)

func NewBuilder(file string) (*Transporter, error) {
	t := &Transporter{}
	t.vm = goja.New()
	t.vm.Set("transporter", t)
	t.vm.Set("t", t.vm.Get("transporter"))
	for _, name := range adaptor.RegisteredAdaptors() {
		t.vm.Set(name, buildAdaptor(name))
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
	lastNode   *pipeline.Node
}

func (t *Transporter) Run() error {
	var g group.Group
	p, err := pipeline.NewPipeline(version, t.sourceNode, events.LogEmitter(), 60*time.Second, nil, 10*time.Second)
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

func buildAdaptor(name string) func(map[string]interface{}) *pipeline.Node {
	return func(args map[string]interface{}) *pipeline.Node {
		uuid, _ := uuid.NewV4()
		nodeName := uuid.String()
		if name, ok := args["name"]; ok {
			nodeName = name.(string)
			delete(args, "name")
		}
		if _, ok := args["namespace"]; !ok {
			args["namespace"] = "test./.*/"
		}
		return pipeline.NewNode(nodeName, name, args)
	}
}

func (t *Transporter) Source(call goja.FunctionCall) goja.Value {
	args := exportArgs(call.Arguments)
	t.sourceNode = args[0].(*pipeline.Node)
	t.lastNode = t.sourceNode
	return t.vm.ToValue(t)
}

func (t *Transporter) Transform(call goja.FunctionCall) goja.Value {
	args := exportArgs(call.Arguments)
	node := args[0].(*pipeline.Node)
	t.lastNode.Add(node)
	t.lastNode = node
	return t.vm.ToValue(t)
}

func (t *Transporter) Save(call goja.FunctionCall) goja.Value {
	args := exportArgs(call.Arguments)
	node := args[0].(*pipeline.Node)
	t.lastNode.Add(node)
	t.lastNode = node
	return t.vm.ToValue(t)
}

func exportArgs(args []goja.Value) []interface{} {
	if len(args) == 0 {
		return nil
	}
	out := make([]interface{}, 0, len(args))
	for _, a := range args {
		out = append(out, a.Export())
	}
	return out
}
