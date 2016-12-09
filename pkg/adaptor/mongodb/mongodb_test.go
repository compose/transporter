package mongodb

import (
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

var sorttests = []struct {
	in       interface{}
	sortable bool
}{
	{"IamAstring", true},
	{bson.NewObjectId(), true},
	{time.Now(), true},
	{int64(1000), true},
	{float64(100.9), true},
	{100.5, true},
	{1000, false},
	{false, false},
}

func TestSortable(t *testing.T) {
	for _, st := range sorttests {
		out := sortable(st.in)
		if out != st.sortable {
			t.Errorf("unexpected result for %+v, expected %+v, got %+v", st.in, st.sortable, out)
		}
	}
}

var rawtests = []struct {
	in  string
	out interface{}
}{
	{"IamAstring", "IamAstring"},
	{"584ed2fe56463f2a877b352b", bson.ObjectIdHex("584ed2fe56463f2a877b352b")},
	{"100.5", 100.5},
	{"1000", 1000},
}

func TestRawDataFromString(t *testing.T) {
	for _, rt := range rawtests {
		result := rawDataFromString(rt.in)
		if result != rt.out {
			t.Errorf("unexpected result for %+v, expected %+v, got %+v", rt.in, rt.out, result)
		}
	}

}
