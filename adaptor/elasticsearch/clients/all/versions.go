package all

import (
	// ensures init functions get called
	_ "github.com/compose/transporter/adaptor/elasticsearch/clients/v1"
	_ "github.com/compose/transporter/adaptor/elasticsearch/clients/v2"
	_ "github.com/compose/transporter/adaptor/elasticsearch/clients/v5"
)
