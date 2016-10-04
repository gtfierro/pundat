package common

import (
	"gopkg.in/vmihailenco/msgpack.v2"
	"sync"
	"time"
)

var EmptyTimeseries = []Timeseries{}
var EmptyStatisticTimeseries = []StatisticTimeseries{}

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

type StatisticsReading struct {
	// uint64 timestamp
	Time  time.Time
	Count uint64
	Min   float64
	Mean  float64
	Max   float64
}

type Timeseries struct {
	sync.RWMutex
	Records    []*TimeseriesReading
	Generation uint64
	SrcURI     string
	UUID       UUID
}

// sort by timestamp
func (ts Timeseries) Len() int {
	return len(ts.Records)
}

func (ts Timeseries) Swap(i, j int) {
	ts.Records[i], ts.Records[j] = ts.Records[j], ts.Records[i]
}

func (ts Timeseries) Less(i, j int) bool {
	return ts.Records[i].Time.Before(ts.Records[j].Time)
}

func (ts *Timeseries) AddRecord(rec *TimeseriesReading) {
	ts.Lock()
	ts.Records = append(ts.Records, rec)
	ts.Unlock()
}

func (ts *Timeseries) NumReadings() int {
	ts.RLock()
	defer ts.RUnlock()
	return len(ts.Records)
}

type StatisticTimeseries struct {
	sync.RWMutex
	Records    []*StatisticsReading
	Generation uint64
	SrcURI     string
	UUID       UUID
}

func (ts *StatisticTimeseries) AddRecord(rec *StatisticsReading) {
	ts.Lock()
	ts.Records = append(ts.Records, rec)
	ts.Unlock()
}

func (ts *StatisticTimeseries) NumReadings() int {
	ts.RLock()
	defer ts.RUnlock()
	return len(ts.Records)
}

// sort by timestamp
func (ts StatisticTimeseries) Len() int {
	return len(ts.Records)
}

func (ts StatisticTimeseries) Swap(i, j int) {
	ts.Records[i], ts.Records[j] = ts.Records[j], ts.Records[i]
}

func (ts StatisticTimeseries) Less(i, j int) bool {
	return ts.Records[i].Time.Before(ts.Records[j].Time)
}

type TimeseriesDataGroup interface {
	NumReadings() int
}

// closed on start, open on end: [start, end)
type TimeRange struct {
	StartTime  int64
	EndTime    int64
	Generation uint64
}

type ChangedRange struct {
	Ranges []*TimeRange
	UUID   UUID
}
