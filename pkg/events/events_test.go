package events

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestEvent(t *testing.T) {

	data := []struct {
		in   Event
		want []byte
	}{
		{
			NewBootEvent(12345, "1.2.3", nil),
			[]byte("{\"ts\":12345,\"name\":\"boot\",\"version\":\"1.2.3\"}"),
		},
		{
			NewBootEvent(12345, "1.2.3", map[string]string{"nick": "yay"}),
			[]byte("{\"ts\":12345,\"name\":\"boot\",\"version\":\"1.2.3\",\"endpoints\":{\"nick\":\"yay\"}}"),
		},
		{
			NewMetricsEvent(12345, "nick/yay", 1, 1),
			[]byte("{\"ts\":12345,\"name\":\"metrics\",\"path\":\"nick/yay\",\"records_in\":1,\"records_out\":1}"),
		},
	}

	for _, d := range data {
		ba, err := json.Marshal(d.in)
		if err != nil {
			t.Errorf("got error: %s", err)
			t.FailNow()
		}

		if !reflect.DeepEqual(ba, d.want) {
			t.Errorf("wanted: %s, got: %s", d.want, ba)
		}
	}
}
