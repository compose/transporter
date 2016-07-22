package all

import (
	// Initialize all adapters by importing this package
	_ "git.compose.io/compose/transporter/pkg/adaptor/elasticsearch"
	_ "git.compose.io/compose/transporter/pkg/adaptor/etcd"
	_ "git.compose.io/compose/transporter/pkg/adaptor/file"
	_ "git.compose.io/compose/transporter/pkg/adaptor/mongodb"
	_ "git.compose.io/compose/transporter/pkg/adaptor/postgres"
	_ "git.compose.io/compose/transporter/pkg/adaptor/rethinkdb"
	_ "git.compose.io/compose/transporter/pkg/adaptor/transformer"
)
