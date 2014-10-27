package main

import (
	"log"
	"os"

	// "github.com/MongoHQ/transporter/pkg/application"
	"github.com/MongoHQ/transporter/pkg/application_builder"
)

func main() {
	// flag.Parse()
	log.SetPrefix("transporter: ")
	log.SetFlags(0)

	app, err := application_builder.Build()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	log.Printf("%v", app)
}
