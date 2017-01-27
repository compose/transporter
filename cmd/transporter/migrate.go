package main

import (
	"errors"

	"github.com/compose/transporter/pkg/adaptor"
	"github.com/compose/transporter/pkg/transporter"
)

func runMigrate(args []string) error {
	var configFilename string
	flagset := baseFlagSet("migrate", &configFilename)
	flagset.Usage = usageFor(flagset, "transporter migrate [flags] <pipeline>")
	if err := flagset.Parse(args); err != nil {
		return err
	}

	config, err := LoadConfig(configFilename)
	if err != nil {
		return err
	}

	args = flagset.Args()
	if len(args) <= 0 {
		return errors.New("name of a file to run is required")
	}

	builder, err := NewJavascriptBuilder(config, args[0], "")
	if err != nil {
		return err
	}

	if err = builder.Build(); err != nil {
		return err
	}

	for _, pipeline := range builder.pipelines {
		err := migrateNode(pipeline.Source)
		if err != nil {
			return err
		}
	}

	return nil
}

func migrateNode(node *transporter.Node) error {
	if migrate, ok := node.Adaptor.(adaptor.Migrateable); ok {
		err := migrate.Migrate()
		if err != nil {
			return err
		}
	}
	for _, child := range node.Children {
		err := migrateNode(child)
		if err != nil {
			return err
		}
	}
	return nil
}
