package data

import "gopkg.in/mgo.v2/bson"

type SQLData map[string]interface{}

type BSONData bson.M

type MapData map[string]interface{}

type CommandData string
