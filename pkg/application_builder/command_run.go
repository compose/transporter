package application_builder

import (
	"errors"

	"github.com/MongoHQ/transporter/pkg/application"
)

var (
	runCommand = &Command{
		Name:  "run",
		Short: "Run a transporter application",
		Run: func(builder ApplicationBuilder, args []string) (application.Application, error) {
			if len(args) == 0 {
				return nil, errors.New("no filename specified")
			}

			js, err := NewJavascriptBuilder(args[0])
			if err != nil {
				return nil, err
			}

			return js.Build()
		},
	}
)
