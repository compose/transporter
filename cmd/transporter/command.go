package main

import (
	"flag"
	"fmt"

	"github.com/mitchellh/cli"
)

// a list of generators for all the subcommand types
var subCommandFactory = map[string]cli.CommandFactory{
	"list": func() (cli.Command, error) {
		return &listCommand{}, nil
	},
	"test": func() (cli.Command, error) {
		return &testCommand{}, nil
	},
	"run": func() (cli.Command, error) {
		return &runCommand{}, nil
	},
	"eval": func() (cli.Command, error) {
		return &evalCommand{}, nil
	},
}

// listCommand loads the config, and lists the configured nodes
type listCommand struct {
	configFilename string
}

func (c *listCommand) Synopsis() string {
	return "list all configured nodes"
}

func (c *listCommand) Help() string {
	return `Usage: trasporter list --config [file]

   list the nodes that have been configured in the configuration yaml`
}

func (c *listCommand) Run(args []string) int {
	var configFilename string
	cmdFlags := flag.NewFlagSet("list", flag.ContinueOnError)
	cmdFlags.Usage = func() { c.Help() }
	cmdFlags.StringVar(&configFilename, "config", "config.yaml", "config file")
	cmdFlags.Parse(args)

	config, err := LoadConfig(configFilename)
	if err != nil {
		fmt.Println(err)
		return 1
	}
	fmt.Printf("%-20s %-15s %s\n", "Name", "Type", "URI")
	for n, v := range config.Nodes {
		fmt.Printf("%-20s %-15s %s\n", n, v.Type, v.URI)
	}

	return 0
}

// runCommand loads a js file, and compiles and runs a
// javascript pipeline defined therein
type runCommand struct {
}

func newRunCommand() (cli.Command, error) {
	return &runCommand{}, nil
}

func (c *runCommand) Help() string {
	return `Usage: transporter run [--config file] <filename>

Run a transporter transporter application by sourcing a file containing the javascript application
and compiling the transporter pipeline`
}

func (c *runCommand) Synopsis() string {
	return "Run a transporter application loaded from a file"
}

func (c *runCommand) Run(args []string) int {
	var configFilename string
	cmdFlags := flag.NewFlagSet("run", flag.ContinueOnError)
	cmdFlags.Usage = func() { c.Help() }
	cmdFlags.StringVar(&configFilename, "config", "config.yaml", "config file")
	cmdFlags.Parse(args)

	config, err := LoadConfig(configFilename)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	if len(cmdFlags.Args()) == 0 {
		fmt.Println("Error: A name of a file to run is required")
		return 1
	}

	builder, err := NewJavascriptBuilder(config, cmdFlags.Args()[0], "")
	if err != nil {
		fmt.Println(err)
		return 1
	}
	if err = builder.Build(); err != nil {
		fmt.Println(err)
		return 1
	}

	if err = builder.Run(); err != nil {
		fmt.Println(err)
		return 1
	}
	return 0
}

// runCommand loads a js file, and compiles and runs a
// javascript pipeline defined therein
type testCommand struct {
}

func (c *testCommand) Help() string {
	return `Usage: transporter test [--config file]  <filename>

Compile a transporter application by sourcing an application file, but do not run it`
}

func (c *testCommand) Synopsis() string {
	return "display the compiled nodes without starting a pipeline"
}

func (c *testCommand) Run(args []string) int {
	var configFilename string
	cmdFlags := flag.NewFlagSet("test", flag.ContinueOnError)
	cmdFlags.Usage = func() { c.Help() }
	cmdFlags.StringVar(&configFilename, "config", "config.yaml", "config file")
	cmdFlags.Parse(args)

	config, err := LoadConfig(configFilename)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	if len(cmdFlags.Args()) == 0 {
		fmt.Println("Error: A name of a file to test is required")
		return 1
	}

	builder, err := NewJavascriptBuilder(config, cmdFlags.Args()[0], "")
	if err != nil {
		fmt.Println(err)
		return 1
	}
	if err = builder.Build(); err != nil {
		fmt.Println(err)
		return 1
	}
	fmt.Println(builder)
	return 0
}

// evalCommand compiles inline javascript into a transporter pipeline,
// and runs it
type evalCommand struct {
}

func (c *evalCommand) Help() string {
	return `Usage: transporter eval [--config file]  <javascript>

Compile a transporter application by evaluating the given javascript`
}

func (c *evalCommand) Synopsis() string {
	return "Eval javascript to build and run a transporter application"
}

func (c *evalCommand) Run(args []string) int {
	var configFilename string
	cmdFlags := flag.NewFlagSet("run", flag.ContinueOnError)
	cmdFlags.Usage = func() { c.Help() }
	cmdFlags.StringVar(&configFilename, "config", "config.yaml", "config file")
	cmdFlags.Parse(args)

	config, err := LoadConfig(configFilename)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	if len(cmdFlags.Args()) == 0 {
		fmt.Println("Error: A string to evaluate is required")
		return 1
	}

	builder, err := NewJavascriptBuilder(config, "", cmdFlags.Args()[0])
	if err != nil {
		fmt.Println(err)
		return 1
	}
	if err = builder.Build(); err != nil {
		fmt.Println(err)
		return 1
	}

	if err = builder.Run(); err != nil {
		fmt.Println(err)
		return 1
	}

	return 0
}
