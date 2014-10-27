package mejson

import (
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"time"

	"labix.org/v2/mgo/bson"
)

type M map[string]interface{}

type I interface{}

type S []interface{}

func Unmarshal(m map[string]interface{}) (result bson.M, err error) {
	return M(m).bson()
}

func (m M) bson() (result bson.M, err error) {
	result = bson.M{}

	for key, value := range m {
		switch v := value.(type) {
		case []interface{}:
			result[key], err = S(v).bson()
			if err != nil {
				return
			}

		case map[string]interface{}:
			if !M(v).isExtended() {
				result[key], err = M(v).bson()
				if err != nil {
					return
				}
			} else {
				if oid, ok := M(v).oid(); ok {
					result[key] = oid
				} else if date, ok := M(v).date(); ok {
					result[key] = date
				} else if timestamp, ok := M(v).timestamp(); ok {
					result[key] = timestamp
				} else if binary, ok := M(v).binary(); ok {
					result[key] = binary
				} else if regex, ok := M(v).regex(); ok {
					result[key] = regex
				} else {
					result[key], err = M(v).bson() // it's ugly to repeat this clause here
					if err != nil {
						return
					}
				}
			}
		default:
			result[key] = v
		}
	}

	return
}

func (m M) isExtended() bool {
	if len(m) != 1 && len(m) != 2 {
		return false
	}

	for k, _ := range m {
		if k[0] != '$' {
			return false
		}
	}

	return true
}

/* $oid type */
func (m M) oid() (oid bson.ObjectId, ok bool) {
	if len(m) != 1 {
		return
	}
	if value, contains := m["$oid"]; contains {
		if hex, isstr := value.(string); isstr && bson.IsObjectIdHex(hex) {
			oid = bson.ObjectIdHex(hex)
			ok = true
		}
	}
	return
}

// RFC3339Nano with a numeric zone
const ISO8601 = "2006-01-02T15:04:05.999999999-0700"

/* $date type */
func (m M) date() (date time.Time, ok bool) {
	if len(m) != 1 {
		return
	}

	if value, contains := m["$date"]; contains {
		var millis int
		var err error

		switch m := value.(type) {
		case int:
			millis = m
		case int64:
			millis = int(m)
		case int32:
			millis = int(m)
		case float64:
			millis = int(m)
		case float32:
			millis = int(m)
		// The MongoDB JSON parser currently does not support loading
		// ISO-8601 strings representing dates prior to the Unix epoch.
		// When formatting pre-epoch dates and dates past what your
		// system’s time_t type can hold, the following format is used:
		// { "$date" : { "$numberLong" : "<dateAsMilliseconds>" } }
		case map[string]interface{}:
			if value, contains := m["$numberLong"]; contains {
				millis, err = strconv.Atoi(value.(string))
			} else {
				return
			}
		// In Strict mode, <date> is an ISO-8601 date format with a
		// mandatory time zone field following the template
		// YYYY-MM-DDTHH:mm:ss.mmm<+/-Offset>.
		case string:
			date, err = time.Parse(ISO8601, m)
			ok = (err == nil)
			return
		default:
			return
		}
		ok = (err == nil)
		if ok {
			date = time.Unix(0, int64(millis)*int64(time.Millisecond))
		}
		return
	}

	return
}

/* bsonify a mongo Timestamp */
func (m M) timestamp() (timestamp bson.MongoTimestamp, ok bool) {
	if len(m) != 1 {
		return
	}

	if value, contains := m["$timestamp"]; contains {
		if ts, ismap := value.(map[string]interface{}); ismap {
			t, isok := ts["t"]
			if !isok {
				return
			}
			tt, isok := t.(int)
			if !isok {
				return
			}

			i, isok := ts["i"]
			if !isok {
				return
			}
			ii, isok := i.(int)
			if !isok {
				return
			}

			ok = true
			var concat int64
			concat = int64(uint64(tt)<<32 | uint64(ii))
			timestamp = bson.MongoTimestamp(concat)
		}
	}

	return
}

/* bsonify a binary data type */
func (m M) binary() (binary bson.Binary, ok bool) {

	if len(m) != 2 {
		return
	}
	kind, kindok := getBinaryKind(m)
	if !kindok {
		return
	}
	data, dataok := getBinaryData(m)
	if !dataok {
		return
	}
	binary.Kind = kind
	binary.Data = data
	ok = true

	return
}

func getBinaryKind(m map[string]interface{}) (kind byte, ok bool) {
	v, contains := m["$type"]
	if !contains {
		return
	}
	hexstr, isstr := v.(string)
	if !isstr {
		return
	}
	hexbytes, err := hex.DecodeString(hexstr)
	if err != nil || len(hexbytes) != 1 {
		return
	}
	kind = hexbytes[0]
	ok = true
	return
}

func getBinaryData(m map[string]interface{}) (data []byte, ok bool) {
	v, contains := m["$binary"]
	if !contains {
		return
	}
	binarystr, isstr := v.(string)
	if !isstr {
		return
	}
	bytes, err := base64.StdEncoding.DecodeString(binarystr)
	if err != nil {
		return
	}
	data = bytes
	ok = true
	return
}

func (m M) regex() (regex bson.RegEx, ok bool) {
	if len(m) != 2 && len(m) != 1 {
		return
	}

	_pattern, ok := m["$regex"]
	if !ok {
		return
	}
	pattern, ok := _pattern.(string)
	if !ok {
		return
	}

	_options, ok := m["$options"]
	if !ok {
		_options = ""
	}
	options, ok := _options.(string)
	if !ok {
		return
	}

	return bson.RegEx{
		Pattern: pattern,
		Options: options,
	}, true
}

/* BSONify a slice of somethings */
func (s S) bson() (out S, err error) {
	out = make(S, len(s))
	for k, v := range s {
		switch elem := v.(type) {
		case []interface{}:
			out[k], err = S(elem).bson()
			if err != nil {
				return
			}
		case map[string]interface{}:
			if !M(elem).isExtended() {
				out[k], err = M(elem).bson()
				if err != nil {
					continue
				}
			} else {
				if oid, ok := M(elem).oid(); ok {
					out[k] = oid
				} else if date, ok := M(elem).date(); ok {
					out[k] = date
				} else if timestamp, ok := M(elem).timestamp(); ok {
					out[k] = timestamp
				} else if binary, ok := M(elem).binary(); ok {
					out[k] = binary
				} else if regex, ok := M(elem).regex(); ok {
					out[k] = regex
				} else {
					out[k], err = M(elem).bson() // it's ugly to repeat this clause here
					if err != nil {
						continue
					}
				}
			}

		default:
			out[k] = elem
		}
	}
	return
}
