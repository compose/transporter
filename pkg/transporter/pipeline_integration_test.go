// +build integration

package transporter

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/compose/transporter/pkg/adaptor"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	mongoUri = "mongodb://localhost/test"
)

// set up some local files
func setupFiles(in, out string) {
	// setup files
	os.Remove(out)
	os.Remove(in)

	fh, _ := os.Create(out)
	defer func() {
		fh.Close()
	}()
	fh.WriteString("{\"_id\":\"546656989330a846dc7ce327\",\"test\":\"hello world\"}\n")
}

// set up local mongo
func setupMongo() {
	// setup mongo
	mongoSess, _ := mgo.Dial(mongoUri)
	collection := mongoSess.DB("testOut").C("coll")
	collection.DropCollection()

	for i := 0; i <= 5; i += 1 {
		collection.Insert(bson.M{"index": i})
	}

	mongoSess.Close()
	mongoSess, _ = mgo.Dial(mongoUri)
	collection = mongoSess.DB("testIn").C("coll")
	collection.DropCollection()
	mongoSess.Close()
}

//
//
//

func TestFileToFile(t *testing.T) {
	var (
		inFile  = "/tmp/crapIn"
		outFile = "/tmp/crapOut"
	)

	setupFiles(inFile, outFile)

	// create the source node and attach our sink
	outNode := NewNode("localfileout", "file", adaptor.Config{"uri": "file://" + outFile}).
		Add(NewNode("localfilein", "file", adaptor.Config{"uri": "file://" + inFile}))

	// create the pipeline
	p, err := NewDefaultPipeline(outNode, "", "", "", 100*time.Millisecond)
	if err != nil {
		t.Errorf("can't create pipeline, got %s", err.Error())
		t.FailNow()
	}

	// run it
	err = p.Run()
	if err != nil {
		t.Errorf("error running pipeline, got %s", err.Error())
		t.FailNow()
	}

	// compare the files
	sourceFile, _ := os.Open(outFile)
	sourceSize, _ := sourceFile.Stat()
	defer sourceFile.Close()
	sinkFile, _ := os.Open(inFile)
	sinkSize, _ := sinkFile.Stat()
	defer sinkFile.Close()

	if sourceSize.Size() == 0 || sourceSize.Size() != sinkSize.Size() {
		t.Errorf("Incorrect file size\nexp %d\ngot %d", sourceSize.Size(), sinkSize.Size())
	}
}

//
//
//

func TestMongoToMongo(t *testing.T) {
	setupMongo()

	var (
		inNs  = "testIn.coll"
		outNs = "testOut.coll"
	)

	// create the source node and attach our sink
	outNode := NewNode("localOutmongo", "mongo", adaptor.Config{"uri": mongoUri, "namespace": outNs}).
		Add(NewNode("localInmongo", "mongo", adaptor.Config{"uri": mongoUri, "namespace": inNs}))

	// create the pipeline
	p, err := NewDefaultPipeline(outNode, "", "", "", 100*time.Millisecond)
	if err != nil {
		t.Errorf("can't create pipeline, got %s", err.Error())
		t.FailNow()
	}

	// run it
	err = p.Run()
	if err != nil {
		t.Errorf("error running pipeline, got %s", err.Error())
		t.FailNow()
	}

	// connect to mongo and compare results
	mongoSess, err := mgo.Dial(mongoUri)
	if err != nil {
		t.Error(err.Error())
	}
	defer mongoSess.Close()

	collOut := mongoSess.DB("testOut").C("coll")
	collIn := mongoSess.DB("testIn").C("coll")

	// are the counts the same?
	outCount, _ := collOut.Count()
	inCount, _ := collIn.Count()

	if outCount != inCount {
		t.Errorf("Incorrect collection size\nexp %d\ngot %d\n", outCount, inCount)
	}

	// iterate over the results and compare the documents
	var result bson.M
	iter := collIn.Find(bson.M{}).Iter()
	for iter.Next(&result) {
		var origDoc bson.M
		err := collOut.Find(bson.M{"_id": result["_id"]}).One(&origDoc)
		if err != nil {
			t.Errorf("Unable to locate source doc +%v\n", result)
			t.FailNow()
		}
		if !reflect.DeepEqual(result, origDoc) {
			t.Errorf("Documents do not match\nexp %v\n, got %v\n", origDoc, result)
		}
	}

	// clean up
	mongoSess.DB("testOut").C("coll").DropCollection()
	mongoSess.DB("testIn").C("coll").DropCollection()

}
