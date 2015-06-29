package main

import (
	"log"
	"os"

	"github.com/compose/transporter/pkg/transporter"
	"github.com/mitchellh/cli"
)

func main() {

	log.SetPrefix("transporter: ")
	log.SetFlags(0)

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
		log.Println(err)
	}

	os.Exit(exitStatus)

}
