package clients

import (
	"net/http"
	"net/url"

	"github.com/compose/transporter/client"
	"github.com/hashicorp/go-version"
)

// VersionedClient encapsulates a version.Constraints and Creator func that can be stopred in
// the Clients map.
type VersionedClient struct {
	Constraint version.Constraints
	Creator    Creator
}

// Creator defines the func signature expected for any implementing Writer
type Creator func(*ClientOptions) (client.Writer, error)

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
	Index      string
	ParentId   string
}
