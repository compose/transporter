package main

import (
	"fmt"
	"os"

	"github.com/compose/transporter/adaptor"
)

func runInit(args []string) error {
	flagset := baseFlagSet("init")
	flagset.Usage = usageFor(flagset, "transporter init [source] [sink]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	args = flagset.Args()
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments provided, expected 2, got %d", len(args))
	}
	fmt.Println("Writing pipeline.js...")
	appFileHandle, err := os.Create(defaultPipelineFile)
	if err != nil {
		return err
	}
	defer appFileHandle.Close()
	nodeName := "source"
	for _, name := range args {
		a, _ := adaptor.GetAdaptor(name, map[string]interface{}{})
		if d, ok := a.(adaptor.Describable); ok {
			appFileHandle.WriteString(fmt.Sprintf("var %s = %s(%s)\n\n", nodeName, name, d.SampleConfig()))
			nodeName = "sink"
		} else {
			return fmt.Errorf("adaptor '%s' did not provide a sample config", name)
		}
	}
	appFileHandle.WriteString(`t.Source(source).Save(sink)`)
	appFileHandle.WriteString(`// t.Source("source", source).Save("sink", sink)`)
	appFileHandle.WriteString(`// t.Source("source", source, "/.*/").Save("sink", sink, "/.*/")`)
	appFileHandle.WriteString("\n")
	return nil
}
