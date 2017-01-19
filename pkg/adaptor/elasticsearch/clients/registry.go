package clients

import (
	"net/http"
	"net/url"
	"sync"

	"github.com/compose/transporter/pkg/client"
	"github.com/hashicorp/go-version"
)

type VersionedClient struct {
	Constraint version.Constraints
	Creator    Creator
}

// Creator is something
type Creator func(chan struct{}, *sync.WaitGroup, *ClientOptions) (client.Writer, error)

// Clients contains the map of versioned clients
var Clients = map[string]*VersionedClient{}

// Add exposes the ability for each versioned client to register itself for use
func Add(v string, constraint version.Constraints, creator Creator) {
	Clients[v] = &VersionedClient{constraint, creator}
}

type ClientOptions struct {
	URLs       []string
	UserInfo   *url.Userinfo
	HTTPClient *http.Client
	Path       string
}
