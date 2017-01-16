package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/log"

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
	"about": func() (cli.Command, error) {
		return &aboutCommand{}, nil
	},
}

func buildFlagSet(setName string, configFilename *string, args []string, usage func()) *flag.FlagSet {
	cmdFlags := flag.NewFlagSet(setName, flag.ContinueOnError)
	log.AddFlags(cmdFlags)
	cmdFlags.Usage = usage
	cmdFlags.StringVar(configFilename, "config", "", "config file")
	cmdFlags.Parse(args)
	return cmdFlags
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
	buildFlagSet("list", &configFilename, args, func() { c.Help() })

	config, err := LoadConfig(configFilename)
	if err != nil {
		fmt.Println(err)
		return 1
	}
	fmt.Printf("%-20s %-15s %s\n", "Name", "Type", "URI")
	for n, v := range config.Nodes {
		kind, _ := v["type"].(string)
		uri, _ := v["uri"].(string)
		fmt.Printf("%-20s %-15s %s\n", n, kind, uri)
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

Run a transporter application by sourcing a file containing the javascript application
and compiling the transporter pipeline`
}

func (c *runCommand) Synopsis() string {
	return "Run a transporter application loaded from a file"
}

func (c *runCommand) Run(args []string) int {
	var configFilename string
	cmdFlags := buildFlagSet("run", &configFilename, args, func() { c.Help() })

	config, err := LoadConfig(configFilename)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	if len(cmdFlags.Args()) == 0 {
		fmt.Println("err: a name of a file to run is required")
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

	stop := make(chan struct{})
	shutdown := make(chan struct{})
	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGHUP)
	go func() {
		select {
		case sig := <-signals:
			if sig == os.Interrupt {
				close(shutdown)
			}
		case <-stop:
			close(shutdown)
		}
	}()

	if err = builder.Run(stop); err != nil {
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
	cmdFlags := buildFlagSet("test", &configFilename, args, func() { c.Help() })

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
	return `Usage: transporter eval [--config file] <javascript>

Compile a transporter application by evaluating the given javascript`
}

func (c *evalCommand) Synopsis() string {
	return "Eval javascript to build and run a transporter application"
}

func (c *evalCommand) Run(args []string) int {
	var configFilename string
	cmdFlags := buildFlagSet("eval", &configFilename, args, func() { c.Help() })

	config, err := LoadConfig(configFilename)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	if len(cmdFlags.Args()) == 0 {
		fmt.Println("err: a string to evaluate is required")
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

	shutdown := make(chan struct{})
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		log.Infoln("Listening for signals")

		// Block until one of the signals above is received
		select {
		case <-signalCh:
			log.Infoln("Signal received, attempting clean shutdown...")
			go func() {
				close(shutdown)
			}()
		}

		// Block again until another signal is received, a shutdown timeout elapses,
		// or the Command is gracefully closed
		log.Infoln("Waiting for clean shutdown...")
		select {
		case <-signalCh:
			log.Infoln("second signal received, initializing hard shutdown")
		case <-time.After(time.Second * 30):
			log.Infoln("time limit reached, initializing hard shutdown")
			// case <-cmd.Closed:
			// 	log.Infoln("pipeline shutdown completed")
		}
	}()

	if err = builder.Run(shutdown); err != nil {
		fmt.Println(err)
		return 1
	}

	return 0
}

type aboutCommand struct{}

func (c *aboutCommand) Help() string {
	return `Usage: transporter about [adaptor]

display information about the included database adaptors.
specifying the adaptor will display the adaptors configuration options
`
}

func (c *aboutCommand) Synopsis() string {
	return "Show information about database adaptors"
}

func (c *aboutCommand) Run(args []string) int {
	if len(args) > 0 {
		creator, ok := adaptor.Adaptors[args[0]]
		if !ok {
			fmt.Printf("no adaptor named '%s' exists\n", args[0])
			return 1
		}
		adaptor.Adaptors = map[string]adaptor.Creator{args[0]: creator}
	}

	for name, creator := range adaptor.Adaptors {
		dummyAdaptor, err := creator(nil, "", adaptor.Config{"uri": "test", "namespace": "test.test"})
		if err != nil {
			fmt.Printf("unable to create adaptor '%s', %s\n", name, err.Error())
			return 1
		}
		if d, ok := dummyAdaptor.(adaptor.Describable); ok {
			fmt.Printf("%s - %s\n", name, d.Description())
		}
	}
	return 0
}
