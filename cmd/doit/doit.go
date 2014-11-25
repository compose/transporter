package main

import (
	"fmt"
	"os"

	"github.com/compose/transporter/pkg/transporter"
)

func main() {
	api := transporter.Api{MetricsInterval: 10000, Uri: "http://requestb.in/1b02tpv1"}

	source := transporter.NewNode("name1", "mongo", map[string]interface{}{"uri": "mongodb://localhost/boom", "namespace": "boom.foo", "debug": true})
	sink1 := transporter.NewNode("crapfile", "file", map[string]interface{}{"uri": "stdout://"})
	sink2 := transporter.NewNode("crapfile2", "file", map[string]interface{}{"uri": "stdout://"})

	source.Attach(sink1)
	source.Attach(sink2)

	pipeline, err := transporter.NewPipeline(source, api)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Printf("pipeline:%+v\n", pipeline)
	pipeline.Run()
	fmt.Println("done")
}
