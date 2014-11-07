package node

/*
 * the basic configuration of a transporter
 * TODO not sure whether this should live here, or live in pkg/application
 */
type Config struct {
	Api struct {
		Uri string `json:"uri"`
	} `json: "api"`
	Nodes map[string]Node
}
