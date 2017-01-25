package main

import "errors"

func runEval(args []string) error {
	var configFilename string
	flagset := baseFlagSet("eval", &configFilename)
	flagset.Usage = usageFor(flagset, "transporter eval [flags] <javascript>")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	config, err := LoadConfig(configFilename)
	if err != nil {
		return err
	}

	args = flagset.Args()
	if len(args) <= 0 {
		return errors.New("a string to evaluate is required")
	}

	builder, err := NewJavascriptBuilder(config, "", args[0])
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
