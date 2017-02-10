package clients

import (
	"reflect"
	"testing"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/message"
	version "github.com/hashicorp/go-version"
)

var (
	mock = &MockWriter{}
)

type MockWriter struct{}

func (w *MockWriter) Write(msg message.Msg) func(client.Session) error {
	return func(s client.Session) error {
		return nil
	}
}

func TestAdd(t *testing.T) {
	constraint, _ := version.NewConstraint(">= 0.1, < 1.0")
	vc := &VersionedClient{
		Constraint: constraint,
		Creator:    mockCreator,
	}
	Add("v0", constraint, mockCreator)
	actual := Clients["v0"]
	if !reflect.DeepEqual(actual.Constraint, vc.Constraint) {
		t.Errorf("wrong Constraint, expected %+v, got %+v", vc.Constraint, actual.Constraint)
	}

	actualClient, err := actual.Creator(nil)
	if err != nil {
		t.Fatalf("call to actual.Creator failed, %s", err)
	}
	if !reflect.DeepEqual(actualClient, mock) {
		t.Errorf("wrong Creator, expected %+v, got %+v", mock, actualClient)
	}
}

func mockCreator(opts *ClientOptions) (client.Writer, error) {
	return mock, nil
}
