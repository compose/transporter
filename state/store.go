package state

// Store defines the set of methods an implementing State store must provide.
type Store interface {
	Apply(State) error
	All() ([]State, error)
}
