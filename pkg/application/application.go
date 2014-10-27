package application

type Application interface {
	Run() error
	Stop() error
}
