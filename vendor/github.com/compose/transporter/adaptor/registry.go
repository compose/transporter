package adaptor

// Creator defines the init structure for an adaptor
type Creator func() Adaptor

// Adaptors stores a map of adaptors by name
var adaptors = map[string]Creator{}

// Add should be called in init func of adaptor
func Add(name string, creator Creator) {
	adaptors[name] = creator
}

// GetAdaptor looks up an adaptor by name and then init's it with the provided Config.
// returns ErrNotFound if the provided name was not registered.
func GetAdaptor(name string, conf Config) (Adaptor, error) {
	creator, ok := adaptors[name]
	if ok {
		a := creator()
		err := conf.Construct(a)
		return a, err
	}
	return nil, ErrNotFound{name}
}

// RegisteredAdaptors returns a slice of the names of every adaptor registered.
func RegisteredAdaptors() []string {
	all := make([]string, 0)
	for i := range adaptors {
		all = append(all, i)
	}
	return all
}

// Adaptors returns an non-initialized adaptor and is best used for doing assertions to see if
// the Adaptor supports other interfaces
func Adaptors() map[string]Adaptor {
	all := make(map[string]Adaptor)
	for name, c := range adaptors {
		a := c()
		all[name] = a
	}
	return all
}
