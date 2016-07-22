package adaptor

import (
	_ "git.compose.io/compose/transporter/pkg/message/adaptor/elasticsearch"
	_ "git.compose.io/compose/transporter/pkg/message/adaptor/file"
	// _ "git.compose.io/compose/transporter/pkg/message/adaptor/influxdb"
	_ "git.compose.io/compose/transporter/pkg/message/adaptor/mongodb"
	_ "git.compose.io/compose/transporter/pkg/message/adaptor/rethinkdb"
	_ "git.compose.io/compose/transporter/pkg/message/adaptor/transformer"
)
