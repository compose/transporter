package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/compose/transporter/offset"
	"github.com/compose/transporter/pipeline"
)

func TestNewBuilder(t *testing.T) {
	rand.Seed(time.Now().Unix())
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("nodetest%d", rand.Int31()))
	os.MkdirAll(dataDir, 0777)
	defer os.RemoveAll(dataDir)

	a := buildAdaptor("mongodb")(map[string]interface{}{"uri": "mongo://localhost:27017"})
	source, err := pipeline.NewNodeWithOptions(
		"source", a.name, defaultNamespace,
		pipeline.WithClient(a.a),
		pipeline.WithReader(a.a),
		pipeline.WithCommitLog(dataDir, 1024*1024*10248),
	)
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	om, err := offset.NewLogManager(dataDir, "source/sink")
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	a = buildAdaptor("elasticsearch")(map[string]interface{}{"uri": "http://localhost:9200"})
	_, err = pipeline.NewNodeWithOptions(
		"sink", a.name, defaultNamespace,
		pipeline.WithClient(a.a),
		pipeline.WithWriter(a.a),
		pipeline.WithParent(source),
		pipeline.WithOffsetManager(om),
		pipeline.WithTransforms(
			[]*pipeline.Transform{
				&pipeline.Transform{
					Name:     "trans",
					Fn:       buildFunction("transformer")(map[string]interface{}{"filename": "pipeline.js"}),
					NsFilter: regexp.MustCompile(defaultNamespace),
				},
			},
		),
	)
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	expected := "Transporter:\n"
	expected += source.String()

	builder, err := newBuilder("testdata/test_pipeline.js")
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	actual := builder.String()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("misconfigured transporter\nexpected:\n%s\ngot:\n%s", expected, actual)
	}
}

func TestNewBuilderWithEnv(t *testing.T) {
	rand.Seed(time.Now().Unix())
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("nodetest%d", rand.Int31()))
	os.MkdirAll(dataDir, 0777)
	defer os.RemoveAll(dataDir)

	os.Setenv("TEST_MONGO_URI", "mongo://localhost:27017")
	a := buildAdaptor("mongodb")(map[string]interface{}{"uri": "mongo://localhost:27017"})
	source, err := pipeline.NewNodeWithOptions(
		"source", a.name, defaultNamespace,
		pipeline.WithClient(a.a),
		pipeline.WithReader(a.a),
		pipeline.WithCommitLog(dataDir, 1024*1024*10248),
	)
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	om, err := offset.NewLogManager(dataDir, "source/sink")
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	a = buildAdaptor("elasticsearch")(map[string]interface{}{"uri": "http://localhost:9200"})
	_, err = pipeline.NewNodeWithOptions(
		"sink", a.name, defaultNamespace,
		pipeline.WithClient(a.a),
		pipeline.WithWriter(a.a),
		pipeline.WithParent(source),
		pipeline.WithOffsetManager(om),
		pipeline.WithTransforms(
			[]*pipeline.Transform{
				&pipeline.Transform{
					Name:     "trans",
					Fn:       buildFunction("transformer")(map[string]interface{}{"filename": "pipeline.js"}),
					NsFilter: regexp.MustCompile(defaultNamespace),
				},
			},
		),
	)
	if err != nil {
		t.Fatalf("unexpected error, %s\n", err)
	}

	expected := "Transporter:\n"
	expected += source.String()

	builder, err := newBuilder("testdata/test_pipeline_env.js")
	if err != nil {
		t.Fatalf("unexpected error, %s", err)
	}
	actual := builder.String()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("misconfigured transporter\nexpected:\n%s\ngot:\n%s", expected, actual)
	}
}
