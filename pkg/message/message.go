package message

import (
	"fmt"
	"time"

	"labix.org/v2/mgo/bson"
)

var (
	id_keys = []string{"_id", "id"}
)

type Msg struct {
	Timestamp  int64
	Namespace  string
	Op         OpType
	Id         interface{}
	OriginalId interface{}
	document   bson.M // document is private
	idKey      string // where the original id value is stored, either "_id" or "id"
}

/*
 *
 * Construct a new message
 *
 */
func NewMsg(op OpType, ns string, doc bson.M) *Msg {
	m := &Msg{
		Timestamp: time.Now().Unix(),
		Namespace: ns,
		Op:        op,
	}
	if doc != nil {
		m.document, m.Id = m.extractId(doc)
		m.OriginalId = m.Id
	}

	return m
}

/*
 *
 * seperate the id field from the rest of the document
 * id's will vary from database to database, ie mongodb is '_id',
 * others are 'id'.  \
 *
 */
func (m *Msg) extractId(doc bson.M) (bson.M, interface{}) {
	// fmt.Printf("in doc %+v\n", doc)
	for _, key := range id_keys {
		id, exists := doc[key]
		if exists {
			m.idKey = key
			delete(doc, key)
			return doc, id
			fmt.Printf(" returned doc %+v\n", doc)
		}
	}

	fmt.Printf("id not found %+v\n", doc)
	return doc, nil
}

/*
 *
 * return the original id as a string value
 *
 */
func (m *Msg) IdAsString() string {
	switch t := m.Id.(type) {
	case string:
		return t
	case bson.ObjectId:
		return t.Hex()
	case int32, int64, uint32, uint64:
		return fmt.Sprintf("%d", t)
	case float32, float64:
		return fmt.Sprintf("%f", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

/*
 *
 * return the original doc, unchanged
 *
 */
func (m *Msg) Document() bson.M {
	return m.DocumentWithId(m.idKey)
}

/*
 *
 * set the document and split out the id
 *
 */
func (m *Msg) SetDocument(doc bson.M) {
	m.document, m.Id = m.extractId(doc)
	if m.OriginalId == nil { // if we don't have an original id, then set it here
		m.OriginalId = m.Id
	}
}

/*
 *
 * return the document, with the id field attached to the specified key
 *
 */
func (m *Msg) DocumentWithId(key string) bson.M {
	doc := m.document
	if m.Id != nil {
		doc[key] = m.Id
	}
	return doc
}
