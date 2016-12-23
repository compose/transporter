package clients

import (
	_ "github.com/compose/transporter/pkg/message/adaptor/elasticsearch/clients/all"
)

// Client is the base interface each underlying versioned client must implement
type Client interface {
	NewClient() error
}
