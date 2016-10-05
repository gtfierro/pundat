package common

import (
	"gopkg.in/mgo.v2/bson"
)

type Dict map[string]interface{}

func (d Dict) ToBSON() bson.M {
	var ret = make(bson.M)
	for k, v := range d {
		switch t := v.(type) {
		case Dict:
			ret[k] = t.ToBSON()
		default:
			ret[k] = t
		}
	}
	return ret
}
