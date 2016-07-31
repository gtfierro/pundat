package common

import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

type TimeseriesReading struct {
	// uint64 timestamp
	Time uint64
	UoT  UnitOfTime
	// value associated with this timestamp
	Value float64
}

func (s *TimeseriesReading) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(s.Time, s.Value)
}

func (s *TimeseriesReading) DecodeMsgpack(enc *msgpack.Decoder) error {
	return enc.Decode(&s.Time, &s.Value)
}

func (s *TimeseriesReading) ConvertTime(to_uot UnitOfTime) (err error) {
	guess := GuessTimeUnit(s.Time)
	if to_uot != guess {
		s.Time, err = convertTime(s.Time, guess, to_uot)
		s.UoT = guess
	}
	return
}

type Timeseries struct {
	Records    []*TimeseriesReading
	Generation int64
}
