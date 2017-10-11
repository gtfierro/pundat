package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// we use 1/1/2150 as the "upper bound" for guessing timestamps
const (
	S_HIGH  int64 = 5680281600
	MS_HIGH int64 = 5680281600000
	US_HIGH int64 = 5680281600000000
	NS_HIGH int64 = 5680281600000000000
)

func GuessTimeUnit(val int64) UnitOfTime {
	if val < S_HIGH {
		return UOT_S
	} else if val < MS_HIGH {
		return UOT_MS
	} else if val < US_HIGH {
		return UOT_US
	}
	return UOT_NS
}

var TimeConvertErr = errors.New("Over/underflow error in converting time")

func ParseUOT(units string) (UnitOfTime, error) {
	switch units {
	case "s", "sec", "second", "seconds":
		return UOT_S, nil
	case "us", "usec", "microsecond", "microseconds":
		return UOT_US, nil
	case "ms", "msec", "millisecond", "milliseconds":
		return UOT_MS, nil
	case "ns", "nsec", "nanosecond", "nanoseconds":
		return UOT_NS, nil
	default:
		return UOT_S, fmt.Errorf("Invalid unit %v. Must be s,us,ms,ns", units)
	}
}

// unit of time indicators
type UnitOfTime uint

const (
	// nanoseconds 1000000000
	UOT_NS UnitOfTime = 1
	// microseconds 1000000
	UOT_US UnitOfTime = 2
	// milliseconds 1000
	UOT_MS UnitOfTime = 3
	// seconds 1
	UOT_S UnitOfTime = 4
)

var unitmultiplier = map[UnitOfTime]int64{
	UOT_NS: 1000000000,
	UOT_US: 1000000,
	UOT_MS: 1000,
	UOT_S:  1,
}

// Takes a timestamp with accompanying unit of time 'stream_uot' and
// converts it to the unit of time 'target_uot'
func ConvertTime(time int64, stream_uot, target_uot UnitOfTime) (int64, error) {
	var returnTime int64
	if stream_uot == target_uot {
		return time, nil
	}
	if target_uot < stream_uot { // target/stream is > 1, so we can use int64
		returnTime = time * (unitmultiplier[target_uot] / unitmultiplier[stream_uot])
		if returnTime < time {
			return time, TimeConvertErr
		}
	} else {
		returnTime = time / (unitmultiplier[stream_uot] / unitmultiplier[target_uot])
		if returnTime > time {
			return time, TimeConvertErr
		}
	}
	return returnTime, nil
}

// interprets the time as the given unit and returns that
func TimeAsUnit(t time.Time, unit UnitOfTime) int64 {
	switch unit {
	case UOT_S:
		return t.Unix()
	case UOT_MS:
		return t.UnixNano() / 1000000
	case UOT_US:
		return t.UnixNano() / 1000
	case UOT_NS:
		return t.UnixNano()
	}
	return 0
}

func (u UnitOfTime) String() string {
	switch u {
	case UOT_NS:
		return "ns"
	case UOT_US:
		return "us"
	case UOT_MS:
		return "ms"
	case UOT_S:
		return "s"
	default:
		return ""
	}
}

func (u UnitOfTime) MarshalJSON() ([]byte, error) {
	switch u {
	case UOT_NS:
		return []byte(`"ns"`), nil
	case UOT_US:
		return []byte(`"us"`), nil
	case UOT_MS:
		return []byte(`"ms"`), nil
	case UOT_S:
		return []byte(`"s"`), nil
	default:
		return []byte(`"s"`), nil
	}
}

func (u *UnitOfTime) UnmarshalJSON(b []byte) (err error) {
	str := strings.Trim(string(b), `"`)
	switch str {
	case "ns":
		*u = UOT_NS
	case "us":
		*u = UOT_US
	case "ms":
		*u = UOT_MS
	case "s":
		*u = UOT_S
	default:
		return fmt.Errorf("%v is not a valid UnitOfTime", str)
	}
	return nil
}

func ParseAbsTime(num, units string) (time.Time, error) {
	var d time.Time
	var err error
	i, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return d, err
	}
	uot, err := ParseUOT(units)
	if err != nil {
		return d, err
	}
	unixseconds, err := ConvertTime(i, uot, UOT_S)
	if err != nil {
		return d, err
	}
	tmp, err := ConvertTime(unixseconds, UOT_S, uot)
	if err != nil {
		return d, err
	}
	leftover := i - tmp
	unixns, err := ConvertTime(leftover, uot, UOT_NS)
	if err != nil {
		return d, err
	}
	d = time.Unix(int64(unixseconds), int64(unixns))
	return d, err
}

func ParseReltime(num, units string) (time.Duration, error) {
	var d time.Duration
	i, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return d, err
	}
	d = time.Duration(i)
	switch units {
	case "h", "hr", "hour", "hours":
		d *= time.Hour
	case "m", "min", "minute", "minutes":
		d *= time.Minute
	case "s", "sec", "second", "seconds":
		d *= time.Second
	case "us", "usec", "microsecond", "microseconds":
		d *= time.Microsecond
	case "ms", "msec", "millisecond", "milliseconds":
		d *= time.Millisecond
	case "ns", "nsec", "nanosecond", "nanoseconds":
		d *= time.Nanosecond
	case "d", "day", "days":
		d *= 24 * time.Hour
	default:
		err = fmt.Errorf("Invalid unit %v. Must be h,m,s,us,ms,ns,d", units)
	}
	return d, err
}

// Takes 2 durations and returns the result of them added together
func AddDurations(d1, d2 time.Duration) time.Duration {
	d1nano := d1.Nanoseconds()
	d2nano := d2.Nanoseconds()
	res := d1nano + d2nano
	return time.Duration(res) * time.Nanosecond
}

// Takes a timestamp with accompanying unit of time 'stream_uot' and
// converts it to the unit of time 'target_uot'
func convertTime(time int64, stream_uot, target_uot UnitOfTime) (int64, error) {
	var returnTime int64
	if stream_uot == target_uot {
		return time, nil
	}
	if target_uot < stream_uot { // target/stream is > 1, so we can use int64
		returnTime = time * (unitmultiplier[target_uot] / unitmultiplier[stream_uot])
		if returnTime < time {
			return time, TimeConvertErr
		}
	} else {
		returnTime = time / (unitmultiplier[stream_uot] / unitmultiplier[target_uot])
		if returnTime > time {
			return time, TimeConvertErr
		}
	}
	return returnTime, nil
}

// given a URI of form "/a/b/c/!meta/key", returns key
// note: does not require "meta"
func getURIKey(uri string) string {
	li := strings.LastIndex(uri, "/")
	if li > 0 {
		return uri[li+1:]
	}
	return uri
}
