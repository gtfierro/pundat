package common

import (
	"sync"
	"time"
)

// not associated with a UUID (durandal-specific). Needs
// to be included in a MetadataGroup to make that association.
type MetadataRecord struct {
	Key       string
	Value     interface{}
	SrcURI    string
	TimeValid time.Time
}

type MetadataGroup struct {
	sync.RWMutex
	// key: record.Key, value: record
	Records map[string]*MetadataRecord
	// stream UUID
	UUID UUID
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

func (grp *MetadataGroup) AddRecord(rec *MetadataRecord) {
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
