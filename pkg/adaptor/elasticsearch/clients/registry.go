package clients

import (
	"net/http"
	"net/url"
	"sync"

	"github.com/compose/transporter/pkg/client"
	"github.com/hashicorp/go-version"
)

// VersionedClient encapsulates a version.Constraints and Creator func that can be stopred in
// the Clients map.
type VersionedClient struct {
	Constraint version.Constraints
	Creator    Creator
}

// Creator defines the func signature expected for any implementing client.Writer
type Creator func(chan struct{}, *sync.WaitGroup, *ClientOptions) (client.Writer, error)

// Clients contains the map of versioned clients
var Clients = map[string]*VersionedClient{}

// Add exposes the ability for each versioned client to register itself for use
func Add(v string, constraint version.Constraints, creator Creator) {
	Clients[v] = &VersionedClient{constraint, creator}
}

// ClientOptions defines the available options that can be used to configured the client.Writer
type ClientOptions struct {
	URLs       []string
	UserInfo   *url.Userinfo
	HTTPClient *http.Client
	Path       string
}
