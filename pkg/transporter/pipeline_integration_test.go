// +build integration

package transporter

import (
	"os"
	"reflect"
	"testing"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	testApiConfig = Api{
		Uri:             "http://requestb.in/1430xju1",
		MetricsInterval: 10000,
	}

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
	collection := mongoSess.DB("test").C("outColl")
	collection.DropCollection()

	for i := 0; i <= 5; i += 1 {
		collection.Insert(bson.M{"index": i})
	}

	mongoSess.Close()
	mongoSess, _ = mgo.Dial(mongoUri)
	collection = mongoSess.DB("test").C("inColl")
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
	outNode := NewNode("localfileout", "file", map[string]interface{}{"uri": "file://" + outFile})
	outNode.Attach(NewNode("localfilein", "file", map[string]interface{}{"uri": "file://" + inFile}))

	// create the pipeline
	p, err := NewPipeline(outNode, testApiConfig)
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
		inNs  = "test.inColl"
		outNs = "test.outColl"
	)

	// create the source node and attach our sink
	outNode := NewNode("localOutmongo", "mongo", map[string]interface{}{"uri": mongoUri, "namespace": outNs})
	outNode.Add(NewNode("localInmongo", "mongo", map[string]interface{}{"uri": mongoUri, "namespace": inNs}))

	// create the pipeline
	p, err := NewPipeline(outNode, testApiConfig)
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

	collOut := mongoSess.DB("test").C("outColl")
	collIn := mongoSess.DB("test").C("inColl")

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
	mongoSess.DB("test").C("outColl").DropCollection()
	mongoSess.DB("test").C("inColl").DropCollection()

}
