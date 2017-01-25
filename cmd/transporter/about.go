package main

import (
	"fmt"

	"github.com/compose/transporter/pkg/adaptor"
)

func runAbout(args []string) error {
	flagset := baseFlagSet("about", nil)
	flagset.Usage = usageFor(flagset, "transporter about [adaptor]")

	args = flagset.Args()
	if len(args) > 0 {
		creator, ok := adaptor.Adaptors[args[0]]
		if !ok {
			return fmt.Errorf("no adaptor named '%s' exists", args[0])
		}
		adaptor.Adaptors = map[string]adaptor.Creator{args[0]: creator}
	}

	for name, creator := range adaptor.Adaptors {
		dummyAdaptor, err := creator(nil, "", adaptor.Config{"uri": "test", "namespace": "test.test"})
		if err != nil {
			return fmt.Errorf("unable to create adaptor '%s', %s", name, err)
		}
		if d, ok := dummyAdaptor.(adaptor.Describable); ok {
			fmt.Printf("%s - %s\n", name, d.Description())
		}
	}

	return nil
}
