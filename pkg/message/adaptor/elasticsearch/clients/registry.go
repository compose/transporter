package clients

// Creator is something
type Creator func() Client

// Clients contains the map of versioned clients
var Clients = map[string]Creator{}

// Add exposes the ability for each versioned client to register itself for use
func Add(name string, creator Creator) {
	Clients[name] = creator
}
