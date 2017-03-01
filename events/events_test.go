package events

import (
	"reflect"
	"testing"
)

func TestEvent(t *testing.T) {
	data := []struct {
		in         Event
		want       []byte
		wantString string
	}{
		{
			NewBootEvent(12345, "1.2.3", nil),
			[]byte(`{"ts":12345,"name":"boot","version":"1.2.3"}`),
			`boot map[]`,
		},
		{
			NewBootEvent(12345, "1.2.3", map[string]string{"nick": "yay"}),
			[]byte(`{"ts":12345,"name":"boot","version":"1.2.3","endpoints":{"nick":"yay"}}`),
			`boot map[nick:yay]`,
		},
		{
			NewMetricsEvent(12345, "nick/yay", 1),
			[]byte(`{"ts":12345,"name":"metrics","path":"nick/yay","records":1}`),
			`metrics nick/yay records: 1`,
		},
		{
			NewExitEvent(12345, "1.2.3", nil),
			[]byte(`{"ts":12345,"name":"exit","version":"1.2.3"}`),
			`exit map[]`,
		},
		{
			NewErrorEvent(12345, "test", map[string]string{"hello": "world"}, "something broke"),
			[]byte(`{"ts":12345,"name":"error","path":"test","record":{"hello":"world"},"message":"something broke"}`),
			`error record: map[hello:world], message: something broke`,
		},
	}

	for _, d := range data {
		ba, err := d.in.Emit()
		if err != nil {
			t.Errorf("got error: %s", err)
			t.FailNow()
		}

		if !reflect.DeepEqual(ba, []byte(d.want)) {
			t.Errorf("Emit() failed, wanted: %s, got: %s", d.want, ba)
		}

		if !reflect.DeepEqual(d.in.String(), d.wantString) {
			t.Errorf("String() failed, wanted: %s, got: %s", d.wantString, d.in.String())
		}
	}
}
