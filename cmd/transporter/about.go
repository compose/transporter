package main

import (
	"fmt"

	"github.com/compose/transporter/adaptor"
)

func runAbout(args []string) error {
	flagset := baseFlagSet("about")
	flagset.Usage = usageFor(flagset, "transporter about [adaptor]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	args = flagset.Args()
	var adaptors = adaptor.Adaptors()
	if len(args) > 0 {
		a, _ := adaptor.GetAdaptor(args[0], map[string]interface{}{})
		adaptors = map[string]adaptor.Adaptor{args[0]: a}
	}

	for name, a := range adaptors {
		if d, ok := a.(adaptor.Describable); ok {
			fmt.Printf("%s - %s\n", name, d.Description())
			if len(args) > 0 {
				// We were asked specifically about this
				fmt.Printf("\n Sample configuration:\n%s\n", d.SampleConfig())
			}
		} else {
			fmt.Printf("%s - %s\n", name, "no description available")
		}
	}
	return nil
}
