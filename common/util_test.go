package common

import (
	"testing"
	"time"
)

func TestGuessTimeUnit(t *testing.T) {
	var times = []string{
		"1/1/1995",
		"1/1/2010",
		"1/1/2017",
		"1/1/2149",
	}
	type timetest struct {
		time      time.Time
		unit      string
		timestamp int64
	}
	var testcases []timetest
	for _, t := range times {
		for _, unit := range []string{"s", "ms", "us", "ns"} {
			testcase := timetest{
				unit: unit,
			}
			tt, _ := time.Parse("1/2/2006", t)
			testcase.time = tt
			switch unit {
			case "s":
				testcase.timestamp = tt.Unix()
			case "ms":
				testcase.timestamp = tt.Unix() * 1000
			case "us":
				testcase.timestamp = tt.Unix() * 1000000
			case "ns":
				testcase.timestamp = tt.UnixNano()
			}
			testcases = append(testcases, testcase)

		}
	}

	for _, testcase := range testcases {
		timeunit := GuessTimeUnit(uint64(testcase.timestamp))
		if timeunit.String() != testcase.unit {
			t.Errorf("Parsing timestamp %d (%s) gave unit %s but expected %s", testcase.timestamp, testcase.time, timeunit, testcase.unit)
		}
	}
}
