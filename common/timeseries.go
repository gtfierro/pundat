package common

import (
	"gopkg.in/vmihailenco/msgpack.v2"
	"sync"
	"time"
)

type TimeseriesReading struct {
	// uint64 timestamp
	Time time.Time
	// value associated with this timestamp
	Value float64
}

func (s *TimeseriesReading) EncodeMsgpack(enc *msgpack.Encoder) error {
	//TODO: encode as int64
	return enc.Encode(s.Time, s.Value)
}

func (s *TimeseriesReading) DecodeMsgpack(enc *msgpack.Decoder) error {
	//TODO: encode as int64
	return enc.Decode(&s.Time, &s.Value)
}

type Timeseries struct {
	sync.RWMutex
	Records    []*TimeseriesReading
	Generation int64
	SrcURI     string
}

func (ts *Timeseries) AddRecord(rec *TimeseriesReading) {
	ts.Lock()
	ts.Records = append(ts.Records, rec)
	ts.Unlock()
}
