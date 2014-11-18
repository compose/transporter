// +build integration

package transporter

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	localOutMongoCN      = ConfigNode{Extra: map[string]interface{}{"uri": "mongodb://localhost/test", "namespace": "test.outColl"}, Name: "localOutmongo", Type: "mongo"}
	localInMongoCN       = ConfigNode{Extra: map[string]interface{}{"uri": "mongodb://localhost/test", "namespace": "test.inColl"}, Name: "localInmongo", Type: "mongo"}
	integrationFileOutCN = ConfigNode{Extra: map[string]interface{}{"uri": "file:///tmp/crapOut"}, Name: "localfileout", Type: "file"}
	integrationFileInCN  = ConfigNode{Extra: map[string]interface{}{"uri": "file:///tmp/crapIn"}, Name: "localfilein", Type: "file"}
)

var (
	testApiConfig = Api{
		Uri:             "http://requestb.in/1430xju1",
		MetricsInterval: 10000,
	}
)

var (
	filenameOut = strings.Replace(integrationFileOutCN.Extra["uri"].(string), "file://", "", 1)
	filenameIn  = strings.Replace(integrationFileInCN.Extra["uri"].(string), "file://", "", 1)
)

func TestPipelineRun(t *testing.T) {
	data := []struct {
		setupFn      interface{}
		setupFnArgs  []reflect.Value
		in           *ConfigNode
		transformer  []ConfigNode
		terminalNode *ConfigNode
		testFn       interface{}
		cleanupFn    interface{}
	}{
		{
			setupFileInAndOut,
			[]reflect.Value{reflect.ValueOf(filenameOut), reflect.ValueOf(filenameIn)},
			&integrationFileOutCN,
			nil,
			&integrationFileInCN,
			testFileToFile,
			nil,
		},
		{
			setupMongoToMongo,
			nil,
			&localOutMongoCN,
			nil,
			&localInMongoCN,
			testMongoToMongo,
			cleanupMongo,
		},
	}

	for _, v := range data {
		if v.setupFn != nil {
			result := reflect.ValueOf(v.setupFn).Call(v.setupFnArgs)
			if result[0].Interface() != nil {
				t.Errorf("unable to call setupFn, got %s", result[0].Interface().(error).Error())
				t.FailNow()
			}
		}
		p, err := NewPipeline(*v.in, testApiConfig)
		if err != nil {
			t.Errorf("can't create pipeline, got %s", err.Error())
			t.FailNow()
		}
		if v.terminalNode != nil {
			p.AddTerminalNode(*v.terminalNode)
		}

		err = p.Run()
		if err != nil {
			t.Errorf("error running pipeline, got %s", err.Error())
			t.FailNow()
		}

		result := reflect.ValueOf(v.testFn).Call([]reflect.Value{reflect.ValueOf(t)})
		if result[0].Interface() != nil {
			t.Errorf("unable to call setupFn, got %s", result[0].Interface().(error).Error())
			t.FailNow()
		}

		if v.cleanupFn != nil {
			reflect.ValueOf(v.cleanupFn).Call(nil)
		}

	}
}

func clearAndCreateFiles(outFile, inFile string) (*os.File, error) {
	err := os.Remove(outFile)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		return nil, err
	}
	err = os.Remove(inFile)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		return nil, err
	}
	return os.Create(outFile)
}

func setupFileInAndOut(outFile, inFile string) error {
	inFileOut, err := clearAndCreateFiles(outFile, inFile)
	if err != nil {
		return err
	}
	inFileOut.WriteString("{\"_id\":\"546656989330a846dc7ce327\",\"test\":\"hello world\"}\n")
	inFileOut.Close()
	return nil
}

func testFileToFile(t *testing.T) error {
	sourceFile, _ := os.Open(filenameOut)
	sourceSize, _ := sourceFile.Stat()
	defer sourceFile.Close()
	sinkFile, _ := os.Open(filenameIn)
	sinkSize, _ := sinkFile.Stat()
	defer sinkFile.Close()
	if sourceSize.Size() != sinkSize.Size() {
		t.Errorf("Incorrect file size\nexp %d\ngot %d", sourceSize.Size(), sinkSize.Size())
	}
	return nil
}

func setupMongoToMongo() error {
	mongoSess, err := mgo.Dial(localOutMongoCN.Extra["uri"].(string))
	if err != nil {
		return err
	}
	collection := mongoSess.DB("test").C("outColl")
	if err := collection.DropCollection(); err != nil && err.Error() != "ns not found" {
		return err
	}
	for i, _ := range []int{0, 1, 2, 3, 4, 5} {
		collection.Insert(bson.M{"index": i})
	}
	mongoSess.Close()
	mongoSess, err = mgo.Dial(localInMongoCN.Extra["uri"].(string))
	if err != nil {
		return err
	}
	collection = mongoSess.DB("test").C("inColl")
	if err := collection.DropCollection(); err != nil && err.Error() != "ns not found" {
		return err
	}
	mongoSess.Close()
	return nil
}

func testMongoToMongo(t *testing.T) error {
	mongoSessIn, err := mgo.Dial(localInMongoCN.Extra["uri"].(string))
	if err != nil {
		return err
	}
	defer mongoSessIn.Close()
	collOut := mongoSessIn.DB("test").C("outColl")
	collIn := mongoSessIn.DB("test").C("inColl")
	outCount, _ := collOut.Count()
	inCount, _ := collIn.Count()
	if outCount != inCount {
		t.Errorf("Incorrect collection size\nexp %d\ngot %d\n", outCount, inCount)
	}
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
	if err != nil {
		return err
	}

	return nil
}

func cleanupMongo() {
	mongoSess, _ := mgo.Dial(localOutMongoCN.Extra["uri"].(string))
	collection := mongoSess.DB("test").C("outColl")
	collection.DropCollection()
	mongoSess.Close()
	mongoSess, _ = mgo.Dial(localInMongoCN.Extra["uri"].(string))
	collection = mongoSess.DB("test").C("inColl")
	collection.DropCollection()
}
