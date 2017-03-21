package main

import (
	"os"
	"reflect"
	"regexp"
	"testing"

	"github.com/compose/transporter/pipeline"
)

func TestNewBuilder(t *testing.T) {
	a := buildAdaptor("mongodb")(map[string]interface{}{"uri": "mongo://localhost:27017"})
	source, err := pipeline.NewNode("source", a.name, DefaultNamespace, a.a, nil)
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	a = buildAdaptor("elasticsearch")(map[string]interface{}{"uri": "http://localhost:9200"})
	sink, err := pipeline.NewNode("sink", a.name, DefaultNamespace, a.a, source)
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	transformer := buildFunction("transformer")(map[string]interface{}{"filename": "pipeline.js"})
	sink.Transforms = []*pipeline.Transform{&pipeline.Transform{Name: "trans", Fn: transformer, NsFilter: regexp.MustCompile(DefaultNamespace)}}

	expected := "Transporter:\n"
	expected += source.String()

	builder, err := NewBuilder("testdata/test_pipeline.js")
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	actual := builder.String()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("misconfigured transporter\nexpected:\n%s\ngot:\n%s", expected, actual)
	}
}

func TestNewBuilderWithEnv(t *testing.T) {
	os.Setenv("TEST_MONGO_URI", "mongo://localhost:27017")
	a := buildAdaptor("mongodb")(map[string]interface{}{"uri": "mongo://localhost:27017"})
	source, err := pipeline.NewNode("source", a.name, DefaultNamespace, a.a, nil)
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}
	a = buildAdaptor("elasticsearch")(map[string]interface{}{"uri": "http://localhost:9200"})
	sink, err := pipeline.NewNode("sink", a.name, DefaultNamespace, a.a, source)
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	transformer := buildFunction("transformer")(map[string]interface{}{"filename": "pipeline.js"})
	sink.Transforms = []*pipeline.Transform{&pipeline.Transform{Name: "trans", Fn: transformer, NsFilter: regexp.MustCompile(DefaultNamespace)}}

	expected := "Transporter:\n"
	expected += source.String()

	builder, err := NewBuilder("testdata/test_pipeline_env.js")
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	actual := builder.String()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("misconfigured transporter\nexpected:\n%s\ngot:\n%s", expected, actual)
	}
}
