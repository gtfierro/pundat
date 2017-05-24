package common

import (
	bw2 "github.com/immesys/bw2bind"
	"gopkg.in/mgo.v2/bson"
	"sync"
	"time"
)

var timeFormat = "2006-1-2 15:04:05.000000000 -0700 MST"
var EMPTY_METADATA_RECORD = MetadataRecord{}

// not associated with a UUID (pundat-specific). Needs
// to be included in a MetadataGroup to make that association.
type MetadataRecord struct {
	Key       string
	Value     interface{}
	SrcURI    string
	TimeValid time.Time
	// used for retrieving records
	UUID UUID
}

func RecordFromMessage(msg *bw2.SimpleMessage) *MetadataRecord {
	po := msg.GetOnePODF(bw2.PODFSMetadata)
	if _md, ok := po.(bw2.MetadataPayloadObject); ok {
		md := _md.Value()
		return &MetadataRecord{
			Key:       getURIKey(msg.URI),
			Value:     md.Value,
			SrcURI:    msg.URI,
			TimeValid: time.Unix(0, md.Timestamp),
		}
	}
	return nil
}

func RecordFromMessageKey(msg *bw2.SimpleMessage) MetadataRecord {
	po := msg.GetOnePODF(bw2.PODFSMetadata)
	if _md, ok := po.(bw2.MetadataPayloadObject); ok {
		md := _md.Value()
		return MetadataRecord{
			Key:       getURIKey(msg.URI),
			Value:     md.Value,
			SrcURI:    msg.URI,
			TimeValid: time.Unix(0, md.Timestamp),
		}
	}
	return EMPTY_METADATA_RECORD
}

func RecordFromBson(doc bson.M) *MetadataRecord {
	var (
		key    string
		value  string
		srcuri string
		uuid   string
	)
	if _key, found := doc["key"]; found {
		key = _key.(string)
	}
	if _value, found := doc["value"]; found {
		value = _value.(string)
	}
	if _srcuri, found := doc["srcuri"]; found {
		srcuri = _srcuri.(string)
	}
	if _uuid, found := doc["uuid"]; found {
		uuid = _uuid.(string)
	}
	rec := &MetadataRecord{Key: key, Value: value, SrcURI: srcuri, UUID: ParseUUID(uuid)}
	t, ok := doc["timevalid"].(time.Time)
	if !ok {
		rec.TimeValid = time.Unix(0, 0)
	} else {
		rec.TimeValid = t
	}
	return rec
}

type MetadataGroup struct {
	sync.RWMutex
	// key: record.Key, value: record
	Records map[string]*MetadataRecord
	// stream UUID
	UUID UUID
	Path string
}

func NewMetadataGroup(records ...*MetadataRecord) *MetadataGroup {
	grp := &MetadataGroup{
		Records: make(map[string]*MetadataRecord),
	}
	for _, record := range records {
		grp.Records[record.Key] = record
	}
	return grp
}

func NewEmptyMetadataGroup() *MetadataGroup {
	return &MetadataGroup{
		Records: make(map[string]*MetadataRecord),
	}
}

func GroupFromMessage(msg *bw2.SimpleMessage) *MetadataGroup {
	var grp = NewEmptyMetadataGroup()
	for _, po := range msg.POs {
		if po.IsTypeDF(bw2.PODFSMetadata) {
			if _md, ok := po.(bw2.MetadataPayloadObject); ok {
				md := _md.Value()
				rec := &MetadataRecord{
					Key:       getURIKey(msg.URI),
					Value:     md.Value,
					SrcURI:    msg.URI,
					TimeValid: time.Unix(0, md.Timestamp),
				}
				grp.AddRecord(rec)
			}
		}
	}
	return grp
}

func GroupFromBson(doc bson.M) *MetadataGroup {
	var grp = NewEmptyMetadataGroup()
	if uuid, found := doc["uuid"]; found {
		grp.UUID = ParseUUID(uuid.(string))
		delete(doc, "uuid")
	}
	if path, found := doc["path"]; found {
		grp.Path = path.(string)
		delete(doc, "path")
	}
	for key, value := range doc {
		rec := &MetadataRecord{
			Key:   key,
			Value: value,
		}
		grp.AddRecord(rec)
	}
	return grp
}

func (grp *MetadataGroup) IsEmpty() bool {
	return len(grp.Records) == 0 && grp.UUID.String() == ""
}

func (grp *MetadataGroup) AddRecord(rec *MetadataRecord) {
	if rec.Key == "" {
		return
	}
	grp.Lock()
	grp.Records[rec.Key] = rec
	grp.Unlock()
}

func (grp *MetadataGroup) DelRecords(keys ...string) {
	grp.Lock()
	for _, key := range keys {
		delete(grp.Records, key)
	}
	grp.Unlock()
}

func (grp *MetadataGroup) HasKey(key string) bool {
	grp.RLock()
	_, found := grp.Records[key]
	grp.RUnlock()
	return found
}

func (grp *MetadataGroup) GetKey(key string) *MetadataRecord {
	grp.RLock()
	rec := grp.Records[key]
	grp.RUnlock()
	return rec
}

func (grp *MetadataGroup) Merge(g2 *MetadataGroup) {
	g2.RLock()
	grp.Lock()
	defer g2.RUnlock()
	defer grp.Unlock()

	for _, rec := range g2.Records {
		grp.AddRecord(rec)
	}
}
