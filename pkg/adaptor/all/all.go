package all

import (
	// Initialize all adapters by importing this package
	_ "github.com/compose/transporter/pkg/adaptor/elasticsearch"
	_ "github.com/compose/transporter/pkg/adaptor/file"
	_ "github.com/compose/transporter/pkg/adaptor/mongodb"
	_ "github.com/compose/transporter/pkg/adaptor/postgres"
	_ "github.com/compose/transporter/pkg/adaptor/rethinkdb"
	_ "github.com/compose/transporter/pkg/adaptor/transformer"
)
