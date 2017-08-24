package all

import (
	// blank import to ensure init() gets called for each package so it can
	// be properly registered.
	_ "github.com/compose/transporter/function/gojajs"
	_ "github.com/compose/transporter/function/omit"
	_ "github.com/compose/transporter/function/opfilter"
	_ "github.com/compose/transporter/function/ottojs"
	_ "github.com/compose/transporter/function/pick"
	_ "github.com/compose/transporter/function/pretty"
	_ "github.com/compose/transporter/function/remap"
	_ "github.com/compose/transporter/function/rename"
	_ "github.com/compose/transporter/function/skip"
)
