package node

/*
 * the basic configuration of a transporter
 * TODO not sure whether this should live here, or live in pkg/application
 */
type Config struct {
	Api struct {
		Uri             string `json:"uri" yaml:"uri"`
		MetricsInterval int    `json:"interval" yaml:"interval"`
	} `json: "api" yaml:"api"`
	Nodes map[string]Node
}
