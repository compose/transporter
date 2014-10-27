package mejson

import (
	"encoding/base64"
	"fmt"
	"labix.org/v2/mgo/bson"
	"os"
	"reflect"
	"time"
)

type Mejson map[string]interface{}

func Marshal(in interface{}) (interface{}, error) {
	// short circuit for nil
	if in == nil {
		return nil, nil
	}

	if reflect.TypeOf(in).Kind() == reflect.Slice {
		v := reflect.ValueOf(in)

		slice := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			slice[i] = v.Index(i).Interface()
		}
		return marshalSlice(slice)
	} else {
		switch v := in.(type) {
		case bson.M:
			return marshalMap(v)
		case bson.D:
			// todo write marshaller for doc to ensure serialization order
			return marshalMap(v.Map())
		case bson.Binary:
			return marshalBinary(v), nil
		case bson.ObjectId:
			return marshalObjectId(v), nil
		case time.Time:
			return marshalTime(v), nil
		case bson.RegEx:
			return marshalRegex(v), nil
		case string, int, int64, bool, float64, uint32:
			return v, nil
		default:
			fmt.Fprintf(os.Stderr, "unknown type: %T\n", v)
			return v, nil
		}
	}
}

func marshalSlice(in []interface{}) (interface{}, error) {
	result := make([]interface{}, len(in))
	for idx, value := range in {
		mejson, err := Marshal(value)
		if err != nil {
			return nil, err
		}
		result[idx] = mejson
	}
	return result, nil
}

func marshalMap(in bson.M) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for key, value := range in {
		mejson, err := Marshal(value)
		if err != nil {
			return nil, err
		}
		result[key] = mejson
	}
	return result, nil
}

func marshalObjectId(in bson.ObjectId) map[string]interface{} {
	return map[string]interface{}{"$oid": in.Hex()}
}

func marshalBinary(in bson.Binary) Mejson {
	return map[string]interface{}{
		"$type":   fmt.Sprintf("%x", in.Kind),
		"$binary": base64.StdEncoding.EncodeToString(in.Data),
	}
}

func marshalTime(in time.Time) map[string]interface{} {
	return map[string]interface{}{
		"$date": int(in.UnixNano() / 1e6),
	}
}

func marshalTimestamp(in bson.MongoTimestamp) map[string]interface{} {
	//{ "$timestamp": { "t": <t>, "i": <i> } }
	seconds, iteration := int32(in>>32), int32(in)
	return map[string]interface{}{
		"$timestamp": bson.M{"t": seconds, "i": iteration},
	}
}

func marshalRegex(in bson.RegEx) map[string]interface{} {
	return map[string]interface{}{
		"$regex":   in.Pattern,
		"$options": in.Options,
	}
}
