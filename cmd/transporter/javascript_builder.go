package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/events"
	"github.com/compose/transporter/pkg/transporter"
	"github.com/nu7hatch/gouuid"
	"github.com/robertkrimen/otto"
)

// JavascriptBuilder runs the javascript provided and uses it to compile a
// list of transporter nodes and instantiate a transporter pipeline
type JavascriptBuilder struct {
	file   string
	path   string
	script *otto.Script
	vm     *otto.Otto

	nodes     map[string]Node
	pipelines []*transporter.Pipeline

	err    error
	config Config
}

// NewJavascriptBuilder compiles the supplied javascript and creates a Javascriptbulder
func NewJavascriptBuilder(config Config, file, src string) (*JavascriptBuilder, error) {
	js := &JavascriptBuilder{
		file:      file,
		vm:        otto.New(),
		path:      filepath.Dir(file),
		config:    config,
		nodes:     make(map[string]Node),
		pipelines: make([]*transporter.Pipeline, 0),
	}

	var (
		script *otto.Script
		err    error
	)
	if src != "" {
		script, err = js.vm.Compile("", src)
	} else {
		script, err = js.vm.Compile(file, nil)
	}

	if err != nil {
		return js, err
	}
	js.script = script
	js.vm.Set("Source", js.source)

	return js, nil
}

// source initialize a transporter Node as a source and adds it to the builder's node map.
// Source(..) takes one argument, a javascript hash which generally contains at
// least a name and a namespace property
//   {name: .., namespace: ..}
func (js *JavascriptBuilder) source(call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 1 {
		js.err = fmt.Errorf("Source must be called with 1 arg. (%d given)", len(call.ArgumentList))
		return otto.NullValue()
	}

	node, err := js.findNode(call.Argument(0))
	if err != nil {
		js.err = err
		return otto.NullValue()
	}
	js.nodes[node.UUID] = node // persist this

	nodeObject, err := node.Object()
	if err != nil {
		js.err = err
		return otto.NullValue()
	}

	js.setFunc(nodeObject, "transform", js.transform)
	js.setFunc(nodeObject, "save", js.save)
	return nodeObject.Value()
}

// save adds a sink to the transporter pipeline
// each pipeline can have multiple sinks
func (js *JavascriptBuilder) save(node Node, call otto.FunctionCall) (Node, error) {
	thisNode, err := js.findNode(call.Argument(0))
	if err != nil {
		return node, err
	}
	root := js.nodes[node.RootUUID]

	if node.UUID == root.UUID { // save is being called on a root node
		root.Add(&thisNode)
	} else {
		node.Add(&thisNode) // add the generated not to the `this`
		root.Add(&node)     // add the result to the root
	}

	js.nodes[root.UUID] = root
	return root, err
}

// adds a transform function to the transporter pipeline
// transform takes one argument, which is a path to a transformer file.
func (js *JavascriptBuilder) transform(node Node, call otto.FunctionCall) (Node, error) {
	if !call.Argument(0).IsString() {
		return node, fmt.Errorf("bad arguments, expected string, got %T", call.Argument(0).Class())
	}

	fn, _ := call.Argument(0).Export()

	filename := fn.(string)
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(js.path, filename)
	}
	name, err := uuid.NewV4()
	if err != nil {
		return node, err
	}
	transformer, err := NewNode(name.String(), "transformer", adaptor.Config{"filename": filename})
	if err != nil {
		return node, err
	}

	node.Add(&transformer)

	return transformer, nil
}

// pipelines in javascript are chainable, you take in a pipeline, and you return a pipeline
// we just generalize some of that logic here
func (js *JavascriptBuilder) setFunc(obj *otto.Object, token string, fn func(Node, otto.FunctionCall) (Node, error)) error {
	return obj.Set(token, func(call otto.FunctionCall) otto.Value {
		this, _ := call.This.Export()

		node, err := CreateNode(this)
		if err != nil {
			js.err = err
			return otto.NullValue()
		}

		node, err = fn(node, call)
		if err != nil {
			js.err = err
			return otto.NullValue()
		}

		o, err := node.Object()
		if err != nil {
			js.err = err
			return otto.NullValue()
		}

		js.setFunc(o, "transform", js.transform)
		js.setFunc(o, "save", js.save)

		return o.Value()
	})
}

// find the node from the based ont the hash passed in
// the hash needs to at least have a {name: }property
func (js *JavascriptBuilder) findNode(in otto.Value) (n Node, err error) {
	e, err := in.Export()
	if err != nil {
		return n, err
	}

	rawMap, ok := e.(map[string]interface{})
	if !ok {
		return n, fmt.Errorf("first argument must be an hash. (got %T instead)", in)
	}

	// make sure the hash validates.
	// we need a "name" property, and it must be a string
	if _, ok := rawMap["name"]; !ok {
		return n, fmt.Errorf("hash requires a name")
	}
	sourceString, ok := rawMap["name"].(string)
	if !(ok) {
		return n, fmt.Errorf("hash requires a name")
	}

	val, ok := js.config.Nodes[sourceString]
	if !ok {
		return n, fmt.Errorf("no configured nodes found named %s", sourceString)
	}
	rawMap["uri"] = val.URI

	return NewNode(sourceString, val.Type, rawMap)
}

// emitter examines the config file for api information
// and returns the correct
func (js *JavascriptBuilder) emitter() events.Emitter {
	if js.config.API.URI == "" {
		// no URI set, return a noop emitter
		return events.NewNoopEmitter()
	}

	return events.NewHTTPPostEmitter(js.config.API.URI, js.config.API.Key, js.config.API.Pid)
}

// Build runs the javascript script.
// each call to the Source() in the javascript creates a new JavascriptPipeline struct,
// and transformers and sinks are added with calls to Transform(), and Save().
// the call to Transporter.add(pipeline) adds the JavascriptPipeline to the Builder's js_pipeline property
func (js *JavascriptBuilder) Build() error {
	_, err := js.vm.Run(js.script)
	if js.err != nil {
		return js.err
	}
	if err != nil {
		return err
	}

	// get the interval from the config, or else default to 60 seconds
	var interval time.Duration
	if js.config.API.MetricsInterval == "" {
		interval = 60 * time.Second
	} else {
		interval, err = time.ParseDuration(js.config.API.MetricsInterval)
		if err != nil {
			return fmt.Errorf("can't parse api interval (%s)", err.Error())
		}
	}

	// build each pipeline
	for _, node := range js.nodes {
		n := node.CreateTransporterNode()
		pipeline, err := transporter.NewPipeline(n, js.emitter(), interval)
		if err != nil {
			return err
		}
		js.pipelines = append(js.pipelines, pipeline) // remember this pipeline
	}

	return nil
}

// Run runs each of the transporter pipelines sequentially
func (js *JavascriptBuilder) Run() error {
	for _, p := range js.pipelines {
		err := p.Run()
		if err != nil {
			return err
		}
	}

	return nil
}

// String represents the pipelines as a string
func (js *JavascriptBuilder) String() string {
	out := "TransporterApplication:\n"
	for _, p := range js.pipelines {
		out += fmt.Sprintf("%s", p.String())
	}
	return out
}
