package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/compose/transporter/pkg/node"
	"github.com/robertkrimen/otto"
)

type JavascriptPipeline struct {
	Config       node.Config
	Source       *node.Node          `json:"source"`
	Sink         *node.Node          `json:"sink"`
	Transformers []*node.Transformer `json:"transformers"`
}

func NewJavacriptPipeline(source *node.Node, config node.Config) *JavascriptPipeline {
	jp := &JavascriptPipeline{
		Source:       source,
		Transformers: make([]*node.Transformer, 0),
		Config:       config,
	}

	return jp
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
 * add a transformer function to a pipeline.
 * transformers will be called in fifo order
 */
func (jp *JavascriptPipeline) AddTransformer(t *node.Transformer) {
	jp.Transformers = append(jp.Transformers, t)
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

func NewJavascriptBuilder(config node.Config, file string) (*JavascriptBuilder, error) {
	js := &JavascriptBuilder{file: file, vm: otto.New(), path: filepath.Dir(file), js_pipelines: make([]JavascriptPipeline, 0), app: NewTransporterApplication(config)}

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

	pipeline, err := NewJavacriptPipeline(this_node, js.app.Config).Object()
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
	pipeline.Sink = this_node
	js.js_pipelines = append(js.js_pipelines, pipeline)
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

	n, ok := js.app.Config.Nodes[sourceString]
	if !ok {
		return nil, fmt.Errorf("no configured nodes found named %s", sourceString)
	}
	return &node.Node{Name: n.Name, Type: n.Type, Uri: n.Uri, Namespace: sourceNS}, nil
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
		pipeline := node.NewPipeline(p.Source, p.Sink, p.Config, p.Transformers)
		js.app.AddPipeline(*pipeline)
	}

	return js.app, nil
}
