package adaptor

import (
	_ "github.com/compose/transporter/pkg/message/adaptor/file"
	// _ "github.com/compose/transporter/pkg/message/adaptor/influxdb"
	_ "github.com/compose/transporter/pkg/message/adaptor/rethinkdb"
	_ "github.com/compose/transporter/pkg/message/adaptor/transformer"
)
