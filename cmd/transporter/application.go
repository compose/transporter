package main

type Application interface {
	Run() error
	Stop() error
}
