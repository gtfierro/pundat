package common

import (
	uuid "github.com/pborman/uuid"
)

type UUID uuid.UUID

func ParseUUID(s string) UUID {
	return UUID(uuid.Parse(s))
}

type Stream struct {
	URI        string
	UUID       UUID
	Path       string
	Metadata   *MetadataGroup
	Timeseries *Timeseries
	Objects    *ObjectList
}
