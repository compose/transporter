package main

import (
	"fmt"
	"os"

	"github.com/compose/transporter/pkg/adaptor"
)

func runInit(args []string) error {
	flagset := baseFlagSet("init", nil)
	flagset.Usage = usageFor(flagset, "transporter init [source] [sink]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	args = flagset.Args()
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments provided, expected 2, got %d", len(args))
	}
	fmt.Printf("Writing %s...\n", defaultConfigFile)
	cfgFileHandle, err := os.Create(defaultConfigFile)
	if err != nil {
		return err
	}
	defer cfgFileHandle.Close()
	cfgFileHandle.WriteString("nodes:\n")
	nodeName := "source"
	for _, name := range args {
		creator, ok := adaptor.Adaptors[name]
		if !ok {
			return fmt.Errorf("no adaptor named '%s' exists", name)
		}
		dummyAdaptor, _ := creator(nil, "", adaptor.Config{"uri": "test", "namespace": "test.test"})
		if d, ok := dummyAdaptor.(adaptor.Describable); ok {
			cfgFileHandle.WriteString(fmt.Sprintf("  %s:\n%s\n", nodeName, d.SampleConfig()))
			nodeName = "sink"
		} else {
			return fmt.Errorf("adaptor '%s' did not provide a sample config", name)
		}
	}
	fmt.Println("Writing pipeline.js...")
	appFileHandle, err := os.Create("pipeline.js")
	if err != nil {
		return err
	}
	defer appFileHandle.Close()
	appFileHandle.WriteString(`Source({name:"source", namespace:"test./.*/"}).save({name:"sink", namespace:"test./.*/"})`)
	appFileHandle.WriteString("\n")
	return nil
}
