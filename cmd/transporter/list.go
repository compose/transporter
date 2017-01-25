package main

import "fmt"

func runList(args []string) error {
	var configFilename string
	flagset := baseFlagSet("run", &configFilename)
	flagset.Usage = usageFor(flagset, "transporter list [flags]")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	config, err := LoadConfig(configFilename)
	if err != nil {
		return err
	}

	fmt.Printf("%-20s %-15s %s\n", "Name", "Type", "URI")
	for n, v := range config.Nodes {
		kind, _ := v["type"].(string)
		uri, _ := v["uri"].(string)
		fmt.Printf("%-20s %-15s %s\n", n, kind, uri)
	}

	return nil
}
