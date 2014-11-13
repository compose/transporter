package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/compose/transporter/pkg/transporter"
	"github.com/robertkrimen/otto"
)

type JavascriptPipeline struct {
	Nodes []transporter.ConfigNode
}

func NewJavacriptPipeline(source transporter.ConfigNode) *JavascriptPipeline {
	return &JavascriptPipeline{Nodes: []transporter.ConfigNode{source}}
}

/*
 * create a new pipeline from a value, such as what we would get back
 * from an otto.Value.  basically a pipeline that has lost it's identify,
 * and been interfaced{}
 */
func InterfaceToPipeline(val interface{}) (JavascriptPipeline, error) {
	t := JavascriptPipeline{}
	ba, err := json.Marshal(val)

	if err != nil {
		return t, err
	}

	err = json.Unmarshal(ba, &t)
	return t, err
}

/*
 * turn this pipeline into an otto Object
 */
func (t *JavascriptPipeline) Object() (*otto.Object, error) {
	vm := otto.New()
	ba, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}

	return vm.Object(fmt.Sprintf(`(%s)`, string(ba)))
}

/*
 * add a node to a pipeline.
 * nodes are called in fifo order
 */
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

func NewJavascriptBuilder(config transporter.Config, file string) (*JavascriptBuilder, error) {
	js := &JavascriptBuilder{file: file, vm: otto.New(), path: filepath.Dir(file), js_pipelines: make([]JavascriptPipeline, 0), app: NewTransporterApplication(config)}

	script, err := js.vm.Compile(file, nil)
	if err != nil {
		return js, err
	}
	js.script = script
	js.vm.Set("Source", js.source)
	js.vm.Set("Transporter", js.transporter)

	return js, nil
}

/*
 * Create a transporter app
 */
func (js *JavascriptBuilder) transporter(call otto.FunctionCall) otto.Value {
	val, err := js.vm.Object(`({})`)
	if err != nil {
		js.err = err
		return otto.NullValue()
	}
	val.Set("add", js.add)
	return val.Value()
}

/*
 * Add a pipeline to the application
 */
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

/*
 * initialize a transporter pipeline.
 * don't keep any global transporter state, we may end up having multiple transporters
 */
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
	this_node.Role = transporter.SOURCE

	pipeline, err := NewJavacriptPipeline(this_node).Object()
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
func (js *JavascriptBuilder) save(pipeline JavascriptPipeline, call otto.FunctionCall) (JavascriptPipeline, error) {
	this_node, err := js.findNode(call.Argument(0))
	if err != nil {
		return pipeline, err
	}
	this_node.Role = transporter.SINK
	pipeline.AddNode(this_node)

	return pipeline, err
}

/*
 * adds a transform function to the pipeline
 */
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

	transformer := transporter.ConfigNode{
		Name:  "generate a uuid",
		Type:  "transformer",
		Extra: map[string]interface{}{"filename": filename},
	}

	pipeline.AddNode(transformer)
	return pipeline, nil
}

/*
 * pipelines in javascript are chainable, you take in a pipeline, and you return a pipeline
 * we just generalize some of that logic here
 */
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

/*
 *
 * find the node from the based ont the hash passed in
 *
 */
func (js *JavascriptBuilder) findNode(in otto.Value) (n transporter.ConfigNode, err error) {
	e, err := in.Export()
	if err != nil {
		return n, err
	}

	m, ok := e.(map[string]interface{})
	if !ok {
		return n, fmt.Errorf("first argument to transport must be an hash. (got %T instead)", in)
	}

	sourceString, ok := m["name"].(string)
	sourceNS, ok1 := m["namespace"].(string)
	if !(ok && ok1) {
		return n, fmt.Errorf("source hash requires both a 'source', and a 'namespace'")
	}

	n, ok = js.app.Config.Nodes[sourceString]
	if !ok {
		return n, fmt.Errorf("no configured nodes found named %s", sourceString)
	}
	return transporter.ConfigNode{Name: n.Name, Type: n.Type, Extra: map[string]interface{}{"namespace": sourceNS, "uri": n.Uri}}, nil
}

/*
 * Run the javascript environment, pipelines are accumulated in the struct
 */
func (js *JavascriptBuilder) Build() (Application, error) {
	_, err := js.vm.Run(js.script)
	if js.err != nil {
		return nil, js.err
	}
	if err != nil {
		return nil, err
	}
	for _, p := range js.js_pipelines {
		// create a new pipeline with the source
		pipeline, err := transporter.NewPipeline(js.app.Config, p.Nodes[0])
		if err != nil {
			return js.app, err
		}
		// TODO add all the subsequent nodes.  this could probably also happen inside the js 'save' and 'transform' methods, but this works for now.
		for _, n := range p.Nodes[1:] {
			if err = pipeline.AddNode(n); err != nil {
				return js.app, err
			}
		}
		js.app.AddPipeline(*pipeline)
	}

	return js.app, nil
}
