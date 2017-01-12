package main

import (
	"os"

	_ "github.com/compose/transporter/pkg/adaptor/all"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/transporter"
	"github.com/mitchellh/cli"
)

func main() {
	c := cli.NewCLI("transporter", transporter.VERSION)

	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		"list":  subCommandFactory["list"],
		"run":   subCommandFactory["run"],
		"eval":  subCommandFactory["eval"],
		"test":  subCommandFactory["test"],
		"about": subCommandFactory["about"],
	}

	exitStatus, err := c.Run()
	if err != nil {
		log.Infoln(err)
	}

	os.Exit(exitStatus)

}
