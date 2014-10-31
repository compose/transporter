package application_builder

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/MongoHQ/transporter/pkg/application"
	"github.com/MongoHQ/transporter/pkg/node"
)

type ApplicationBuilder struct {
	Nodes []node.Node

	// command to run
	Command *Command

	// Running Config
	Config Config

	// path to the config file
	config_path string
}

func Build() (application.Application, error) {
	builder := ApplicationBuilder{}

	err := builder.flagParse()
	if err != nil {
		builder.usage()
		return nil, err
	}

	err = builder.loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Config Error: %s\n", err)
	}

	// builder.Nodes = make([]*node.Node, len(builder.Config.Nodes))
	// for idx, n := range builder.Config.Nodes {
	// 	builder.Nodes[idx] = node.NewNode(n)
	// }

	builder.Nodes = builder.Config.Nodes
	return builder.Command.Run(builder, builder.Command.Flag.Args())
}

/*
 *
 * flag parsing related functions
 *
 */
func (a *ApplicationBuilder) flagParse() error {
	flag.StringVar(&a.config_path, "config", "", "path to the config yaml")
	flag.Usage = a.usage
	flag.Parse()

	// make sure we're valid
	for _, c := range commands {
		if c.Name == flag.Arg(0) {
			c.Flag.Parse(flag.Args()[1:])

			a.Command = c
			return nil
		}
	}
	return errors.New("Command not found")
}

// TODO this should be cleaned up
// we need a way to have a builder usage/help, and each command will need a usage/help as well
func (a *ApplicationBuilder) usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "transporter [global arguments] command [arguments]\n\n")
	for _, v := range commands {
		fmt.Fprintf(os.Stderr, "    %-8s  %s\n", v.Name, v.Short)
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "global arguments: \n")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr)
}
