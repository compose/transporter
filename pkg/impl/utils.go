package impl

import (
	"fmt"
)

func getExtraValue(extra map[string]interface{}, key string) (string, error) {
	val, ok := extra[key]
	if !ok {
		return "", fmt.Errorf("%s not defined", key)
	}
	s, ok := val.(string)
	if !ok {
		return s, fmt.Errorf("%s not a string", key)
	}
	return s, nil
}
