package skip

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"

	"github.com/compose/transporter/function"
	"github.com/compose/transporter/message"
)

type unknownOperatorError struct {
	Op string
}

func (e unknownOperatorError) Error() string {
	return fmt.Sprintf("unkown operator, %s", e.Op)
}

type wrongTypeError struct {
	Wanted string
	Got    string
}

func (e wrongTypeError) Error() string {
	return fmt.Sprintf("value is of incompatible type, wanted %s, got %s", e.Wanted, e.Got)
}

func init() {
	function.Add(
		"skip",
		func() function.Function {
			return &skip{}
		},
	)
}

type skip struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Match    interface{} `json:"match"`
}

func (s *skip) Apply(msg message.Msg) (message.Msg, error) {
	val := msg.Data().Get(s.Field)
	switch s.Operator {
	case "==", "eq", "$eq":
		if reflect.DeepEqual(val, s.Match) {
			return msg, nil
		}
	case "=~":
		if ok, err := regexp.MatchString(s.Match.(string), val.(string)); err != nil || ok {
			return msg, err
		}
	case ">", "gt", "$gt":
		v, m, err := convertForComparison(val, s.Match)
		if err == nil && v > m {
			return msg, err
		}
		return nil, err
	case ">=", "gte", "$gte":
		v, m, err := convertForComparison(val, s.Match)
		if err == nil && v >= m {
			return msg, err
		}
		return nil, err
	case "<", "lt", "$lt":
		v, m, err := convertForComparison(val, s.Match)
		if err == nil && v < m {
			return msg, err
		}
		return nil, err
	case "<=", "lte", "$lte":
		v, m, err := convertForComparison(val, s.Match)
		if err == nil && v <= m {
			return msg, err
		}
		return nil, err
	default:
		return nil, unknownOperatorError{s.Operator}
	}
	return nil, nil
}

func convertForComparison(in1, in2 interface{}) (float64, float64, error) {
	float1, err := convertToFloat(in1)
	if err != nil {
		return math.NaN(), math.NaN(), err
	}
	float2, err := convertToFloat(in2)
	if err != nil {
		return math.NaN(), math.NaN(), err
	}
	return float1, float2, nil
}

func convertToFloat(in interface{}) (float64, error) {
	switch i := in.(type) {
	case float64:
		return i, nil
	case int:
		return float64(i), nil
	case string:
		return strconv.ParseFloat(i, 0)
	default:
		return math.NaN(), wrongTypeError{"float64 or int", fmt.Sprintf("%T", i)}
	}

}
