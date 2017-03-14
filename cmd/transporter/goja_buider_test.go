package main

import (
	"os"
	"reflect"
	"testing"
)

func TestNewBuilder(t *testing.T) {
	source := buildAdaptor("mongodb")(map[string]interface{}{"name": "source", "uri": "mongo://localhost:27017"})
	transformer := buildAdaptor("transformer")(map[string]interface{}{"name": "trans", "filename": "pipeline.js"})
	source.Add(transformer)
	transformer.Add(buildAdaptor("elasticsearch")(map[string]interface{}{"name": "sink", "uri": "http://localhost:2900"}))
	expected := "Transporter:\n"
	expected += source.String()

	builder, err := NewBuilder("testdata/test_pipeline.js")
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	actual := builder.String()
	if reflect.DeepEqual(actual, expected) {
		t.Errorf("misconfigured transporter\nexpected:\n%s\ngot:\n%s", expected, actual)
	}
}

func TestNewBuilderWithEnv(t *testing.T) {
	os.Setenv("TEST_MONGO_URI", "mongo://localhost:27017")
	source := buildAdaptor("mongodb")(map[string]interface{}{"name": "source", "uri": "mongo://localhost:27017"})
	transformer := buildAdaptor("transformer")(map[string]interface{}{"name": "trans", "filename": "pipeline.js"})
	source.Add(transformer)
	transformer.Add(buildAdaptor("elasticsearch")(map[string]interface{}{"name": "sink", "uri": "http://localhost:2900"}))
	expected := "Transporter:\n"
	expected += source.String()

	builder, err := NewBuilder("testdata/test_pipeline_env.js")
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	actual := builder.String()
	if reflect.DeepEqual(actual, expected) {
		t.Errorf("misconfigured transporter\nexpected:\n%s\ngot:\n%s", expected, actual)
	}

}
