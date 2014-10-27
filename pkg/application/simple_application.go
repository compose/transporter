package application

type SimpleApplication struct {
	runner func() error
}

func NewSimpleApplication(runner func() error) *SimpleApplication {
	return &SimpleApplication{runner: runner}
}

func (a *SimpleApplication) Run() error {
	return a.runner()
}

func (a *SimpleApplication) Stop() error {
	return nil
}
