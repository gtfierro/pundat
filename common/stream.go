package common

import (
	uuid "github.com/pborman/uuid"
	"gopkg.in/mgo.v2/bson"
)

type UUID uuid.UUID

func (u UUID) String() string {
	return uuid.UUID(u).String()
}

func (u UUID) Bytes() (ret [16]byte) {
	copy(ret[:], []byte(uuid.UUID(u))[:16])
	return
}

func (u UUID) GetBSON() (interface{}, error) {
	return u.String(), nil
}

func (u *UUID) SetBSON(raw bson.Raw) error {
	var s string
	err := raw.Unmarshal(s)
	*u = UUID(s)
	return err
}

func (u UUID) MarshalJSON() ([]byte, error) {
	return []byte(u.String()), nil
}

func (u UUID) UnmarshalJSON(d []byte) error {
	u = UUIDFromByteSlice(d)
	return nil
}

func UUIDFromBytes(b [16]byte) UUID {
	return UUID(uuid.UUID(b[:]))
}

func UUIDFromByteSlice(b []byte) UUID {
	return UUID(uuid.UUID(b))
}

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
