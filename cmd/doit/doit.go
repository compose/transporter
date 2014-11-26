package main

import (
	"fmt"
	"os"

	"github.com/compose/transporter/pkg/transporter"
)

func main() {
	source := transporter.NewNode("name1", "mongo", map[string]interface{}{"uri": "mongodb://localhost/boom", "namespace": "boom.foo", "debug": true})
	fmt.Printf("%+v\n", source)

	sink1 := transporter.NewNode("crapfile", "file", map[string]interface{}{"uri": "stdout://"})
	fmt.Printf("%+v\n", sink1)

	source.Attach(sink1)
	fmt.Printf("%+v\n", source)

	sink2 := transporter.NewNode("crapfile2", "file", map[string]interface{}{"uri": "stdout://"})
	source.Attach(sink2)
	fmt.Printf("%+v\n", source)

	api := transporter.Api{MetricsInterval: 10000, Uri: "http://requestb.in/1b02tpv1"}

	pipeline, err := transporter.NewPipeline(source, api)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Printf("%s", pipeline.String())
	pipeline.Run()
}
