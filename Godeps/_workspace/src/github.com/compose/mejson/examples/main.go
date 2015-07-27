package main

import (
	"fmt"
	"github.com/compose/mejson"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func marsh() {
	session, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("test").C("people")
	result := []bson.M{}
	err = c.Find(bson.M{}).All(&result)
	if err != nil {
		panic(err)
	}
	bytes, err := mejson.Marshal(result)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", bytes)
}

func bsonify() {
	in := map[string]interface{}{
		"_id": map[string]interface{}{
			"$oid": "123412341234123412341234",
		},
		"test": map[string]interface{}{
			"subid": map[string]interface{}{
				"$oid": "123412341234123412341234",
			},
		},
		"time": map[string]interface{}{
			"$date": 1392227630711,
		},
		"timestamp": map[string]interface{}{
			"$timestamp": map[string]interface{}{
				"t": 18,
				"i": 1,
			},
		},
		"binary": map[string]interface{}{
			"$binary": "b2ggaGk=",
			"$type":   "00",
		},
	}
	m, err := mejson.Unmarshal(in)
	if err != nil {
		panic(err)
	}
	fmt.Printf("bson: %+v\n", m)
}

func main() {
	bsonify()
}
