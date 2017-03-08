package main

import (
	"fmt"
)

func runTest(args []string) error {
	var configFilename string
	flagset := baseFlagSet("test", &configFilename)
	flagset.Usage = usageFor(flagset, "transporter test [flags] <pipeline>")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	config, err := LoadConfig(configFilename)
	if err != nil {
		return err
	}

	args = flagset.Args()
	if len(args) <= 0 {
		// Make pipeline.js the default argument
		args = []string{"pipeline.js"}
	}

	builder, err := NewJavascriptBuilder(config, args[0], "")
	if err != nil {
		return err
	}

	if err = builder.Build(); err != nil {
		return err
	}

	fmt.Println(builder)
	return nil
}
