package main

import (
	"errors"
	"flag"
	"log"
)

// registry of available commands
var (
	commands = map[string]*Command{
		"list": listCommand,
		"run":  runCommand,
	}
)

// Command is a description of a subcommand
type Command struct {
	Name  string
	Short string
	Help  string
	Flag  flag.FlagSet

	Run func(ApplicationBuilder, []string) (Application, error)
}

// list the nodes that are configured in the config.yaml
var listCommand = &Command{
	Name:  "list",
	Short: "list all configured nodes",
	Help: `Usage: trasporter --config [file] list

  list the nodes that have been configured in the configuration yaml`,
	Run: func(builder ApplicationBuilder, args []string) (Application, error) {
		return NewSimpleApplication(func() error {
			for _, v := range builder.Config.Nodes {
				log.Println(v)
			}
			return nil
		}), nil
	},
}

var (
	runEval string
)

func init() {
	runCommand.Flag.StringVar(&runEval, "eval", "", "javascript to define a transporter")
}

// run a transporter js applications, and use it to build and run pipelines
var (
	runCommand = &Command{
		Name:  "run",
		Short: "Run a transporter application",
		Help: `Usage: transporter --config [file] run [-eval javascript] [filename]


Run a transporter transporter application by either sourcing a file containing the javascript application, 
or by evaluating the javascript with -eval.`,
		Run: func(builder ApplicationBuilder, args []string) (Application, error) {
			if len(args) == 0 && runEval == "" {
				return nil, errors.New("no filename or javascript specified")
			}
			var (
				js  *JavascriptBuilder
				err error
			)

			if runEval == "" {
				js, err = NewJavascriptBuilder(builder.Config, args[0], "")
				if err != nil {
					return nil, err
				}
			} else {
				js, err = NewJavascriptBuilder(builder.Config, "", runEval)
			}

			return js.Build()
		},
	}
)
