package dots

import (
	"fmt"
	"github.com/immesys/bw2/objects"
	"sort"
	"time"
)

type timeRange struct {
	start time.Time
	end   time.Time
}

func NewTimeRange(start, end time.Time) *timeRange {
	return &timeRange{
		start: start,
		end:   end,
	}
}

// returns true if the two time ranges do not overlap
// if false, then they must overlap
func (rng *timeRange) isDisjoint(rng2 *timeRange) bool {
	return rng.end.Before(rng2.start)
}

// merges 2 time ranges into 1, assuming that they are NOT disjoint
// the range on which this is called is the result, e.g. r1.mergeFrom(r2)
// will place the merged range into r1
func (rng *timeRange) mergeFrom(rng2 *timeRange) {
	if rng.start.After(rng2.start) {
		rng.start = rng2.start
	}
	if rng.end.Before(rng2.end) {
		rng.end = rng2.end
	}
}

func (rng *timeRange) String() string {
	return fmt.Sprintf("Start: %s, End: %s", rng.start, rng.end)
}

// returns true if the given time is between [start,end]
func (rng *timeRange) contains(t time.Time) bool {
	return (rng.start.Before(t) && rng.end.After(t)) || rng.start.Equal(t) || rng.end.Equal(t)
}

// assumes that DOTs are (filled), which is guaranteed by GetDOTChains
// This will first pull the range from each DOT, where the range is defined by
// the creation and expiration time.
func GetTimeRanges(dchain *objects.DChain) *timeRangeCollection {
	col := &timeRangeCollection{
		ranges: []*timeRange{},
	}
	num := dchain.NumHashes()
	for i := 0; i < num; i++ {
		dot := dchain.GetDOT(i)
		col.ranges = append(col.ranges, NewTimeRange(*dot.GetCreated(), *dot.GetExpiry()))
	}
	return col
}

type timeRangeCollection struct {
	ranges []*timeRange
}

// implementing Sort interface
func (col *timeRangeCollection) Len() int {
	return len(col.ranges)
}

func (col *timeRangeCollection) Swap(i, j int) {
	col.ranges[i], col.ranges[j] = col.ranges[j], col.ranges[i]
}

func (col *timeRangeCollection) Less(i, j int) bool {
	return col.ranges[i].start.Before(col.ranges[j].start)
}

// sorts the time ranges in order from earliest start date to latest start date
func (col *timeRangeCollection) Sort() {
	sort.Sort(col)
}

// merges the time ranges
func (col *timeRangeCollection) Compress() {
	// our "result" set
	var compressed []*timeRange
	// each of the ranges we have to work on
	for _, newrng := range col.ranges {
		var merged = false
		// for each of the already merged ranges
		for _, oldrng := range compressed {
			// if our range overlaps with an existing range,
			// we merge it, else we add the new, disjoint, range
			// to our result list
			if !newrng.isDisjoint(oldrng) {
				oldrng.mergeFrom(newrng)
				merged = true
				break
			}
		}
		if !merged {
			compressed = append(compressed, newrng)
		}
	}
	col.ranges = compressed
}

func (col *timeRangeCollection) String() string {
	var res string
	res += fmt.Sprintln("\n┏")
	for _, rng := range col.ranges {
		res += fmt.Sprintln("┣", rng)
	}
	res += fmt.Sprintln("┗")
	return res
}

// merges 2 collections of time ranges into a single range
// col.mergeFrom(col2) puts results in col
func (col *timeRangeCollection) mergeFrom(col2 *timeRangeCollection) {
	var toadd []*timeRange
	for _, rng := range col2.ranges {
		var merged = false
		for _, oldrng := range col.ranges {
			if rng.isDisjoint(oldrng) {
				continue
			}
			oldrng.mergeFrom(rng)
			merged = true
			break
		}
		if !merged {
			toadd = append(toadd, rng)
		}
	}
	col.ranges = append(col.ranges, toadd...)
	col.Sort()
}
