package main

import (
	"log"
	"os"

	"github.com/mitchellh/cli"
)

func main() {

	log.SetPrefix("transporter: ")
	log.SetFlags(0)

	c := cli.NewCLI("transporter", "1.0.0")

	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		"list": newListCommand,
		"run":  newRunCommand,
		"eval": newEvalCommand,
		"test": newTestCommand,
	}

	exitStatus, err := c.Run()
	if err != nil {
		log.Println(err)
	}

	os.Exit(exitStatus)

}
