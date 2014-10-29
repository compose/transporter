package application

type Application interface {
	// String() string
	Run() error
	Stop() error
}
