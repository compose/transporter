package application_builder

import (
	"fmt"
	"log"

	"github.com/MongoHQ/transporter/pkg/application"
	"github.com/robertkrimen/otto"
)

type JavascriptBuilder struct {
	file   string
	script *otto.Script
	vm     *otto.Otto

	err error
}

func NewJavascriptBuilder(file string) (JavascriptBuilder, error) {
	js := JavascriptBuilder{file: file, vm: otto.New()}

	script, err := js.vm.Compile(file, nil)
	if err != nil {
		return js, err
	}
	js.script = script

	js.vm.Set("Transport", js.Transport)

	return js, nil
}

func (js JavascriptBuilder) Transport(call otto.FunctionCall) otto.Value {
	log.Printf("Transport called with %d args", len(call.ArgumentList))
	if len(call.ArgumentList) != 2 {
		js.err = fmt.Errorf("Transporter must be called with 2 args. (%d given)", len(call.ArgumentList))
		return otto.FalseValue()
	}

	source, err := js.source(call.Argument(0))
	if err != nil {
		js.err = err
		return otto.FalseValue()
	}

	log.Printf("source is: %v", source)

	//everything is ok
	return otto.TrueValue()
}

func (js JavascriptBuilder) source(in otto.Value) (map[string]interface{}, error) {
	e, err := in.Export()
	if err != nil {
		return nil, err
	}

	m, ok := e.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("first argument to transport must be an hash. (got %T instead)", in)
	}

	return m, nil
}

func (js JavascriptBuilder) Build() (application.Application, error) {
	_, err := js.vm.Run(js.script)
	if err != nil {
		return nil, err
	}
	if js.err != nil {
		return nil, js.err
	}

	return nil, nil
}
