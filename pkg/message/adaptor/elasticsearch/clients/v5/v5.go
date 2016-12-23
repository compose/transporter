package client

import (
	"github.com/compose/transporter/pkg/message/adaptor/elasticsearch/clients"
	"gopkg.in/olivere/elastic.v5"
)

type Client struct {
	esClient *elastic.Client
}

// NewClient instantiates a new elastic client
func NewClient() error {
	return nil
}

func init() {
	clients.Add("v5", func() Client {
		return &Client{}
	})
}
