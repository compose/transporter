package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/compose/transporter/pkg/transporter"
	"github.com/nu7hatch/gouuid"
	"github.com/robertkrimen/otto"
)

type JavascriptPipeline struct {
	Nodes []transporter.ConfigNode
}

func NewJavacriptPipeline(source transporter.ConfigNode) *JavascriptPipeline {
	return &JavascriptPipeline{Nodes: []transporter.ConfigNode{source}}
}

// create a new pipeline from a interface, such as what we would get back
// from an otto.Value.  basically a pipeline that has lost it's identify,
// and been interfaced{}
func InterfaceToPipeline(val interface{}) (JavascriptPipeline, error) {
	t := JavascriptPipeline{}
	ba, err := json.Marshal(val)

	if err != nil {
		return t, err
	}

	err = json.Unmarshal(ba, &t)
	return t, err
}

// turn this pipeline into an otto Object
func (t *JavascriptPipeline) Object() (*otto.Object, error) {
	vm := otto.New()
	ba, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}

	return vm.Object(fmt.Sprintf(`(%s)`, string(ba)))
}

// add a node to a pipeline.
// nodes are called in fifo order
func (jp *JavascriptPipeline) AddNode(n transporter.ConfigNode) {
	jp.Nodes = append(jp.Nodes, n)
}

type JavascriptBuilder struct {
	file   string
	path   string
	script *otto.Script
	vm     *otto.Otto

	js_pipelines []JavascriptPipeline
	app          *TransporterApplication
	err          error
}

func NewJavascriptBuilder(config transporter.Config, file, src string) (*JavascriptBuilder, error) {
	js := &JavascriptBuilder{file: file, vm: otto.New(), path: filepath.Dir(file), js_pipelines: make([]JavascriptPipeline, 0), app: NewTransporterApplication(config)}

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
	js.vm.Set("Transporter", js.transporter)

	return js, nil
}

// transporter creates a transporter application
func (js *JavascriptBuilder) transporter(call otto.FunctionCall) otto.Value {
	val, err := js.vm.Object(`({})`)
	if err != nil {
		js.err = err
		return otto.NullValue()
	}
	val.Set("add", js.add)
	return val.Value()
}

// add Adds a javascriptPipeline to the application
func (js *JavascriptBuilder) add(call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 1 {
		js.err = fmt.Errorf("Transporter.add must be called with 1 arg.  (%d given)", len(call.ArgumentList))
		return otto.NullValue()
	}

	p, _ := call.Argument(0).Export()

	pipeline, err := InterfaceToPipeline(p)
	if err != nil {
		js.err = err
		return otto.NullValue()
	}

	js.js_pipelines = append(js.js_pipelines, pipeline)
	return otto.TrueValue()
}

// source initialize a transporter pipeline, and adds a source to it.
// Source(..) takes one argument, a javascript hash which generally contains at
// least a name and a namespace property
//   {name: .., namespace: ..}
func (js *JavascriptBuilder) source(call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) != 1 {
		js.err = fmt.Errorf("Transporter must be called with 1 arg. (%d given)", len(call.ArgumentList))
		return otto.NullValue()
	}

	this_node, err := js.findNode(call.Argument(0))
	if err != nil {
		js.err = err
		return otto.NullValue()
	}

	pipeline, err := NewJavacriptPipeline(this_node).Object()
	if err != nil {
		js.err = err
		return otto.NullValue()
	}

	js.SetFunc(pipeline, "transform", js.transform)
	js.SetFunc(pipeline, "save", js.save)
	return pipeline.Value()
}

// save adds a sink to the transporter pipeline
// each pipeline can have multiple sinks
func (js *JavascriptBuilder) save(pipeline JavascriptPipeline, call otto.FunctionCall) (JavascriptPipeline, error) {
	this_node, err := js.findNode(call.Argument(0))
	if err != nil {
		return pipeline, err
	}
	pipeline.AddNode(this_node)

	return pipeline, err
}

// adds a transform function to the transporter pipeline
// transform takes one argument, which is a path to a transformer file.
func (js *JavascriptBuilder) transform(pipeline JavascriptPipeline, call otto.FunctionCall) (JavascriptPipeline, error) {
	if !call.Argument(0).IsString() {
		return pipeline, fmt.Errorf("bad arguments, expected string, got %d.", len(call.Argument(0).Class()))
	}

	fn, _ := call.Argument(0).Export()

	var filename string
	filename = fn.(string)
	if !filepath.IsAbs(fn.(string)) {
		filename = filepath.Join(js.path, filename)
	}
	name, err := uuid.NewV4()
	if err != nil {
		return pipeline, err
	}
	transformer := transporter.ConfigNode{
		Name:  name.String(),
		Type:  "transformer",
		Extra: map[string]interface{}{"filename": filename},
	}

	pipeline.AddNode(transformer)
	return pipeline, nil
}

// pipelines in javascript are chainable, you take in a pipeline, and you return a pipeline
// we just generalize some of that logic here
func (js *JavascriptBuilder) SetFunc(obj *otto.Object, token string, fn func(JavascriptPipeline, otto.FunctionCall) (JavascriptPipeline, error)) error {
	return obj.Set(token, func(call otto.FunctionCall) otto.Value {
		this, _ := call.This.Export()

		pipeline, err := InterfaceToPipeline(this)
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

// find the node from the based ont the hash passed in
// the hash needs to at least have a {name: }property
func (js *JavascriptBuilder) findNode(in otto.Value) (n transporter.ConfigNode, err error) {
	e, err := in.Export()
	if err != nil {
		return n, err
	}

	m, ok := e.(map[string]interface{})
	if !ok {
		return n, fmt.Errorf("first argument to transport must be an hash. (got %T instead)", in)
	}

	// make sure the hash validates.  we need a "name" property
	if _, ok := m["name"]; !ok {
		return n, fmt.Errorf("source hash requires a name")
	}
	sourceString, ok := m["name"].(string)
	if !(ok) {
		return n, fmt.Errorf("source hash requires a name")
	}

	//
	n, ok = js.app.Config.Nodes[sourceString]
	if !ok {
		return n, fmt.Errorf("no configured nodes found named %s", sourceString)
	}
	m["uri"] = n.Uri
	return transporter.ConfigNode{Name: n.Name, Type: n.Type, Extra: m}, nil
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
	for _, p := range js.js_pipelines {

		// create a new pipeline with with the source set to the first element of the Nodes array
		pipeline, err := transporter.NewPipeline(js.app.Config, p.Nodes[0])
		if err != nil {
			return js.app, err
		}

		// all nodes except for the last are added to the Pipeline with pipeline.AddNode()
		for _, n := range p.Nodes[1 : len(p.Nodes)-1] {
			if err = pipeline.AddNode(n); err != nil {
				return js.app, err
			}
		}

		// finally we add the terminal node
		if err = pipeline.AddTerminalNode(p.Nodes[len(p.Nodes)-1]); err != nil {
			return js.app, err
		}

		js.app.AddPipeline(*pipeline)
	}

	return js.app, nil
}
