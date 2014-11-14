package impl

import (
	"encoding/json"
)

type ExtraConfig map[string]interface{}

/*
 * turn the generic map into a proper struct
 */
func (c *ExtraConfig) Construct(conf interface{}) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, conf)
	if err != nil {
		return err
	}
	return nil
}
