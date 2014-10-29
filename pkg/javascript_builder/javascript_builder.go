package javascript_builder

import (
	"fmt"
	"path/filepath"

	"github.com/MongoHQ/transporter/pkg/application"
	"github.com/MongoHQ/transporter/pkg/node"
	"github.com/robertkrimen/otto"
)

type JavascriptBuilder struct {
	file   string
	path   string
	nodes  []*node.Node
	script *otto.Script
	vm     *otto.Otto

	app *application.TransporterApplication
	err error
}

func NewJavascriptBuilder(nodes []*node.Node, file string) (*JavascriptBuilder, error) {
	js := &JavascriptBuilder{file: file, vm: otto.New(), path: filepath.Dir(file), nodes: nodes, app: &application.TransporterApplication{}}

	script, err := js.vm.Compile(file, nil)
	if err != nil {
		return js, err
	}
	js.script = script
	js.vm.Set("Transport", js.transport)

	return js, nil
}

/*
 * initialize a transporter pipeline.
 * don't keep any global transporter state, we may end up having multiple transporters
 */
func (js *JavascriptBuilder) transport(call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 1 {
		js.err = fmt.Errorf("Transporter must be called with 1 arg. (%d given)", len(call.ArgumentList))
		return otto.NullValue()
	}

	this_node, err := js.findNode(call.Argument(0))
	if err != nil {
		js.err = err
		return otto.NullValue()
	}

	pipeline, err := node.NewPipeline(this_node).Object()
	if err != nil {
		js.err = err
		return otto.NullValue()
	}

	js.SetFunc(pipeline, "transform", js.transform)
	js.SetFunc(pipeline, "save", js.save)
	return pipeline.Value()
}

/*
 * save a transporter pipeline
 * this finalized the transporter by adding a sink, and adds the pipeline to the application
 */
func (js *JavascriptBuilder) save(pipeline node.Pipeline, call otto.FunctionCall) (node.Pipeline, error) {
	this_node, err := js.findNode(call.Argument(0))
	if err != nil {
		return pipeline, err
	}
	pipeline.Sink = this_node
	js.app.AddPipeline(pipeline)
	return pipeline, err
}

/*
 * adds a transform function to the pipeline
 */
func (js *JavascriptBuilder) transform(pipeline node.Pipeline, call otto.FunctionCall) (node.Pipeline, error) {
	if !call.Argument(0).IsString() {
		return pipeline, fmt.Errorf("bad arguments, expected string, got %s.", len(call.Argument(0).Class()))
	}

	fn, _ := call.Argument(0).Export()
	transformer := node.NewTransformer()
	var filename string
	filename = fn.(string)
	if !filepath.IsAbs(fn.(string)) {
		filename = filepath.Join(js.path, filename)
	}
	err := transformer.Load(filename)
	if err != nil {
		return pipeline, err
	}

	pipeline.AddTransformer(transformer)
	return pipeline, nil
}

/*
 * pipelines in javascript are chainable, you take in a pipeline, and you return a pipeline
 * we just generalize some of that logic here
 */
func (js *JavascriptBuilder) SetFunc(obj *otto.Object, token string, fn func(node.Pipeline, otto.FunctionCall) (node.Pipeline, error)) error {
	return obj.Set(token, func(call otto.FunctionCall) otto.Value {
		this, _ := call.This.Export()

		pipeline, err := node.InterfaceToPipeline(this)
		if err != nil {
			js.err = err
			return otto.NullValue()
		}

		pipeline, err = fn(pipeline, call)
		if err != nil {
			js.err = err
			return otto.NullValue()
		}

		o, err := pipeline.Object()
		if err != nil {
			js.err = err
			return otto.NullValue()
		}

		js.SetFunc(o, "transform", js.transform)
		js.SetFunc(o, "save", js.save)

		return o.Value()
	})
}

/*
 *
 * find the node from the based ont the hash passed in
 *
 */
func (js *JavascriptBuilder) findNode(in otto.Value) (*node.Node, error) {
	e, err := in.Export()
	if err != nil {
		return nil, err
	}

	m, ok := e.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("first argument to transport must be an hash. (got %T instead)", in)
	}

	sourceString, ok := m["name"].(string)
	sourceNS, ok1 := m["namespace"].(string)
	if !(ok && ok1) {
		return nil, fmt.Errorf("source hash requires both a 'source' and a 'namespace'")
	}

	for _, n := range js.nodes {
		if n.Name == sourceString {
			return &node.Node{Name: n.Name, Type: n.Type, Uri: n.Uri, Namespace: sourceNS}, nil
		}
	}

	return nil, fmt.Errorf("no configured nodes found named %s", sourceString)
}

/*
 * Run the javascript environment, pipelines are accumulated in the struct
 */
func (js *JavascriptBuilder) Build() (application.Application, error) {
	_, err := js.vm.Run(js.script)
	if js.err != nil {
		return nil, js.err
	}
	if err != nil {
		return nil, err
	}

	return js.app, nil
}
