package main

import "errors"

func runRun(args []string) error {
	var configFilename string
	flagset := baseFlagSet("run", &configFilename)
	flagset.Usage = usageFor(flagset, "transporter run [flags] <pipeline>")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	config, err := LoadConfig(configFilename)
	if err != nil {
		return err
	}

	args = flagset.Args()
	if len(args) <= 0 {
		return errors.New("name of a file to run is required")
	}

	builder, err := NewJavascriptBuilder(config, args[0], "")
	if err != nil {
		return err
	}

	if err = builder.Build(); err != nil {
		return err
	}

	if err = builder.Run(); err != nil {
		return err
	}

	return nil
}
