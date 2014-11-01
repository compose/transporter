package main

import (
	"log"
	"os"
)

func main() {
	log.SetPrefix("transporter: ")
	log.SetFlags(0)

	app, err := Build()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	err = app.Run()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
