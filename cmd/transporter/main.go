package main

import (
	"os"
	"os/signal"
	"syscall"

	_ "github.com/compose/transporter/pkg/adaptor/all"
	"github.com/compose/transporter/pkg/log"
	"github.com/compose/transporter/pkg/transporter"
	"github.com/mitchellh/cli"
)

var stop chan struct{}

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

	stop = make(chan struct{})
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

	os.Exit(exitStatus)

}
