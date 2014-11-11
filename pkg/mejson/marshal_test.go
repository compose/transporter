package mejson

import (
	"encoding/json"
	"labix.org/v2/mgo/bson"
	"reflect"
	"testing"
	"time"
)

func TestMarshal(t *testing.T) {

	sample_time, _ := time.Parse(time.RFC3339, "2014-02-19T15:14:41.288Z")

	data := []struct {
		in      interface{}
		want    []byte
		wanterr error
	}{
		{
			bson.ObjectIdHex("52dc18556c528d7736000003"),
			[]byte("{\"$oid\":\"52dc18556c528d7736000003\"}"),
			nil,
		},
		{
			sample_time,
			[]byte("{\"$date\":1392822881288}"),
			nil,
		},
		{
			bson.Binary{Kind: 0x80, Data: []byte("52dc18556c528d7736000003")},
			[]byte("{\"$binary\":\"NTJkYzE4NTU2YzUyOGQ3NzM2MDAwMDAz\",\"$type\":\"80\"}"),
			nil,
		},
		{
			"String",
			[]byte("\"String\""),
			nil,
		},
		{
			true,
			[]byte("true"),
			nil,
		},
		{
			5.1,
			[]byte("5.1"),
			nil,
		},
		{
			nil,
			[]byte("null"),
			nil,
		},
	}

	for _, d := range data {
		mejson, err := Marshal(d.in)
		b, err := json.Marshal(mejson)

		if err != nil && err.Error() != d.wanterr.Error() {
			t.Errorf("wanted!: %s, got: %s", d.wanterr, err)
			t.FailNow()
		}
		if err == nil && err != d.wanterr {
			t.Errorf("wanted: %s, got: %s", d.wanterr, err)
			t.FailNow()
		}
		if !reflect.DeepEqual(b, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, b)
		}
	}
}

func TestMarshalObjectId(t *testing.T) {
	data := []struct {
		in   bson.ObjectId
		want []byte
	}{
		{bson.ObjectIdHex("52dc18556c528d7736000003"), []byte("{\"$oid\":\"52dc18556c528d7736000003\"}")},
		{bson.ObjectIdHex("deadbeefcafedeadbeedcafe"), []byte("{\"$oid\":\"deadbeefcafedeadbeedcafe\"}")},
	}

	for _, d := range data {
		mejson := marshalObjectId(d.in)
		b, err := json.Marshal(mejson)

		if err != nil {
			t.FailNow()
		}
		if !reflect.DeepEqual(b, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, b)
		}
	}
}

func TestMarshalMap(t *testing.T) {
	data := []struct {
		in   bson.M
		want []byte
	}{
		{
			bson.M{"_id": bson.ObjectIdHex("52dc18556c528d7736000003")},
			[]byte("{\"_id\":{\"$oid\":\"52dc18556c528d7736000003\"}}"),
		},
		{
			bson.M{
				"_id": bson.ObjectIdHex("52dc18556c528d7736000003"),
				"s":   "String",
			},
			[]byte("{\"_id\":{\"$oid\":\"52dc18556c528d7736000003\"},\"s\":\"String\"}"),
		},
		{
			bson.M{
				"_id": bson.ObjectIdHex("52dc18556c528d7736000003"),
				"s":   "String",
				"m":   bson.M{"num": 5},
			},
			[]byte("{\"_id\":{\"$oid\":\"52dc18556c528d7736000003\"},\"m\":{\"num\":5},\"s\":\"String\"}"),
		},
		{
			bson.M{
				"_id": bson.ObjectIdHex("52dc18556c528d7736000003"),
				"s":   "String",
				"m": bson.M{
					"num":      5,
					"inner_id": bson.ObjectIdHex("52dc18556c528d7736000005"),
				},
			},
			[]byte("{\"_id\":{\"$oid\":\"52dc18556c528d7736000003\"},\"m\":{\"inner_id\":{\"$oid\":\"52dc18556c528d7736000005\"},\"num\":5},\"s\":\"String\"}"),
		},
		{
			bson.M{
				"_id": bson.ObjectIdHex("52dc18556c528d7736000003"),
				"s":   "String",
				"a":   []int{1, 2, 3},
			},
			[]byte("{\"_id\":{\"$oid\":\"52dc18556c528d7736000003\"},\"a\":[1,2,3],\"s\":\"String\"}"),
		},
	}

	for _, d := range data {
		mejson, err := marshalMap(d.in)
		b, err := json.Marshal(mejson)

		if err != nil {
			t.FailNow()
		}
		if !reflect.DeepEqual(b, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, b)
		}
	}
}

func TestMarshalSlice(t *testing.T) {
	data := []struct {
		in   []interface{}
		want []byte
	}{
		{
			[]interface{}{"one", "two", "three"},
			[]byte("[\"one\",\"two\",\"three\"]"),
		},
		{
			[]interface{}{bson.M{"_id": bson.ObjectIdHex("52dc18556c528d7736000003")}},
			[]byte("[{\"_id\":{\"$oid\":\"52dc18556c528d7736000003\"}}]"),
		},
		{
			[]interface{}{[]string{"one", "two"}, []string{"three", "four"}},
			[]byte("[[\"one\",\"two\"],[\"three\",\"four\"]]"),
		},
	}

	for _, d := range data {
		mejson, err := marshalSlice(d.in)
		b, err := json.Marshal(mejson)

		if err != nil {
			t.FailNow()
		}
		if !reflect.DeepEqual(b, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, b)
		}
	}
}

func TestMarshalBinary(t *testing.T) {
	data := []struct {
		in   bson.Binary
		want []byte
	}{
		{bson.Binary{Kind: 0x80, Data: []byte("52dc18556c528d7736000003")}, []byte("{\"$binary\":\"NTJkYzE4NTU2YzUyOGQ3NzM2MDAwMDAz\",\"$type\":\"80\"}")},
	}

	for _, d := range data {
		mejson := marshalBinary(d.in)
		b, err := json.Marshal(mejson)
		if err != nil {
			t.FailNow()
		}
		if !reflect.DeepEqual(b, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, b)
		}
	}
}

func TestMarshalTime(t *testing.T) {
	sample_time, _ := time.Parse(time.RFC3339, "2014-02-19T15:14:41.288Z")
	sample_time2, _ := time.Parse(time.RFC3339, "2007-02-19T15:14:41.288Z")

	data := []struct {
		in   time.Time
		want []byte
	}{
		{sample_time, []byte("{\"$date\":1392822881288}")},
		{sample_time2, []byte("{\"$date\":1171898081288}")},
	}

	for _, d := range data {
		mejson := marshalTime(d.in)
		b, err := json.Marshal(mejson)

		if err != nil {
			t.FailNow()
		}
		if !reflect.DeepEqual(b, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, b)
		}
	}
}

func TestMarshalRegex(t *testing.T) {
	data := []struct {
		in   bson.RegEx
		want []byte
	}{
		{bson.RegEx{Pattern: "nick", Options: "i"}, []byte("{\"$options\":\"i\",\"$regex\":\"nick\"}")},
	}

	for _, d := range data {
		mejson := marshalRegex(d.in)
		b, err := json.Marshal(mejson)

		if err != nil {
			t.FailNow()
		}
		if !reflect.DeepEqual(b, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, b)
		}
	}
}

func TestMarshalTimestamp(t *testing.T) {
	data := []struct {
		in   bson.MongoTimestamp
		want []byte
	}{
		{bson.MongoTimestamp(0), []byte("{\"$timestamp\":{\"i\":0,\"t\":0}}")},
		{bson.MongoTimestamp(5982128723015499777), []byte("{\"$timestamp\":{\"i\":1,\"t\":1392822881}}")},
	}

	for _, d := range data {
		mejson := marshalTimestamp(d.in)
		b, err := json.Marshal(mejson)

		if err != nil {
			t.FailNow()
		}
		if !reflect.DeepEqual(b, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, b)
		}
	}
}
