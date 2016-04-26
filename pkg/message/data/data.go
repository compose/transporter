package data

import "gopkg.in/mgo.v2/bson"

type SQLData map[string]interface{}

func (s SQLData) AsMap() map[string]interface{} {
	return map[string]interface{}(s)
}

type BSONData bson.M

func (b BSONData) AsMap() map[string]interface{} {
	return map[string]interface{}(b)
}

type MapData map[string]interface{}

func (m MapData) AsMap() map[string]interface{} {
	return map[string]interface{}(m)
}

type CommandData string
