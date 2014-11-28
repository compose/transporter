package main

import (
	"fmt"
	"path/filepath"

	"github.com/compose/transporter/pkg/transporter"
	// "github.com/kr/pretty"
	"github.com/nu7hatch/gouuid"
	"github.com/robertkrimen/otto"
)

type JavascriptBuilder struct {
	file   string
	path   string
	script *otto.Script
	vm     *otto.Otto

	nodes map[string]Node
	app   *TransporterApplication
	err   error
}

func NewJavascriptBuilder(config Config, file, src string) (*JavascriptBuilder, error) {
	js := &JavascriptBuilder{file: file, vm: otto.New(), path: filepath.Dir(file), nodes: make(map[string]Node), app: NewTransporterApplication(config)}

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
	js.nodes[node.Uuid] = node // persist this

	nodeObject, err := node.Object()
	if err != nil {
		js.err = err
		return otto.NullValue()
	}

	js.SetFunc(nodeObject, "transform", js.transform)
	js.SetFunc(nodeObject, "save", js.save)
	return nodeObject.Value()
}

// save adds a sink to the transporter pipeline
// each pipeline can have multiple sinks
func (js *JavascriptBuilder) save(node Node, call otto.FunctionCall) (Node, error) {
	this_node, err := js.findNode(call.Argument(0))
	if err != nil {
		return node, err
	}
	root := js.nodes[node.RootUuid]

	if node.Uuid == root.Uuid { // save is being called on a root node
		root.Add(&this_node)
	} else {
		node.Add(&this_node) // add the generated not to the `this`
		root.Add(&node)      // add the result to the root
	}

	js.nodes[root.Uuid] = root
	return root, err
}

// adds a transform function to the transporter pipeline
// transform takes one argument, which is a path to a transformer file.
func (js *JavascriptBuilder) transform(node Node, call otto.FunctionCall) (Node, error) {
	if !call.Argument(0).IsString() {
		return node, fmt.Errorf("bad arguments, expected string, got %d.", len(call.Argument(0).Class()))
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
	transformer, err := NewNode(name.String(), "transformer", map[string]interface{}{"filename": filename})
	if err != nil {
		return node, err
	}

	node.Add(&transformer)

	return transformer, nil
}

// pipelines in javascript are chainable, you take in a pipeline, and you return a pipeline
// we just generalize some of that logic here
func (js *JavascriptBuilder) SetFunc(obj *otto.Object, token string, fn func(Node, otto.FunctionCall) (Node, error)) error {
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

		js.SetFunc(o, "transform", js.transform)
		js.SetFunc(o, "save", js.save)

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

	val, ok := js.app.Config.Nodes[sourceString]
	if !ok {
		return n, fmt.Errorf("no configured nodes found named %s", sourceString)
	}
	rawMap["uri"] = val.Uri

	return NewNode(sourceString, val.Type, rawMap)
}

// Build runs the javascript script.
// each call to the Source() in the javascript creates a new JavascriptPipeline struct,
// and transformers and sinks are added with calls to Transform(), and Save().
// the call to Transporter.add(pipeline) adds the JavascriptPipeline to the Builder's js_pipeline property
func (js *JavascriptBuilder) Build() (Application, error) {
	_, err := js.vm.Run(js.script)
	if js.err != nil {
		return nil, js.err
	}
	if err != nil {
		return nil, err
	}
	// pretty.Println(js.nodes)
	for _, node := range js.nodes {
		n := node.CreateTransporterNode()

		pipeline, err := transporter.NewDefaultPipeline(n, js.app.Config.Api)
		if err != nil {
			return js.app, err
		}
		js.app.AddPipeline(pipeline)
	}

	return js.app, nil
}
