package all

import (
	// Initialize all adapters by importing this package
	_ "github.com/compose/transporter/adaptor/elasticsearch"
	_ "github.com/compose/transporter/adaptor/file"
	_ "github.com/compose/transporter/adaptor/mongodb"
	_ "github.com/compose/transporter/adaptor/postgres"
	_ "github.com/compose/transporter/adaptor/rabbitmq"
	_ "github.com/compose/transporter/adaptor/rethinkdb"
	_ "github.com/compose/transporter/adaptor/transformer"
)
