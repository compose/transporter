package all

import (
	_ "github.com/compose/transporter/pkg/adaptor/elasticsearch"
	_ "github.com/compose/transporter/pkg/adaptor/file"
	_ "github.com/compose/transporter/pkg/adaptor/mongodb"
	_ "github.com/compose/transporter/pkg/adaptor/rethinkdb"
	_ "github.com/compose/transporter/pkg/adaptor/transformer"
)
