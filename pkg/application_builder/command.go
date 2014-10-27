package application_builder

import (
	"flag"

	"github.com/MongoHQ/transporter/pkg/application"
)

/*
 *
 * valid subcommands for this application
 *
 */

type Command struct {
	Name  string
	Short string
	Flag  flag.FlagSet

	Run func(ApplicationBuilder, []string) (application.Application, error)
}

var (
	commands = map[string]*Command{
		"list": listCommand,
		"run":  runCommand,
	}
)
