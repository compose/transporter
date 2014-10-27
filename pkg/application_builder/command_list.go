package application_builder

import (
	"log"

	"github.com/MongoHQ/transporter/pkg/application"
)

var listCommand = &Command{
	Name:  "list",
	Short: "list all configured nodes",
	Run: func(builder ApplicationBuilder, args []string) (application.Application, error) {
		return application.NewSimpleApplication(func() error {
			for _, v := range builder.Nodes {
				log.Println(v)
			}
			return nil
		}), nil
	},
}
