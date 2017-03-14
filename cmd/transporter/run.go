package main

func runRun(args []string) error {
	flagset := baseFlagSet("run")
	flagset.Usage = usageFor(flagset, "transporter run [flags] <pipeline>")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	args = flagset.Args()
	if len(args) <= 0 {
		// Set to default argument
		args = []string{defaultPipelineFile}
	}

	builder, err := NewBuilder(args[0])
	if err != nil {
		return err
	}

	if err := builder.Run(); err != nil {
		return err
	}

	return nil
}
