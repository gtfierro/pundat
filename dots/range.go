package dots

import (
	"fmt"
	"github.com/immesys/bw2/objects"
	"log"
	"math"
	"time"
)

const Beginning = 0
const EndOfTime = math.MaxInt64

type TimeRange struct {
	Start time.Time
	End   time.Time
}

func NewTimeRange(start, end time.Time) *TimeRange {
	return &TimeRange{
		Start: start,
		End:   end,
	}
}

// returns true if the two time ranges do not overlap
// if false, then they must overlap
func (rng *TimeRange) IsDisjoint(rng2 *TimeRange) bool {
	return rng.End.Before(rng2.Start)
}

// merges 2 time ranges into 1, assuming that they are NOT disjoint
// the range on which this is called is the result, e.g. r1.MergeFrom(r2)
// will place the merged range into r1
func (rng *TimeRange) MergeFrom(rng2 *TimeRange) {
	if rng.Start.After(rng2.Start) {
		rng.Start = rng2.Start
	}
	if rng.End.Before(rng2.End) {
		rng.End = rng2.End
	}
}

func (rng *TimeRange) String() string {
	return fmt.Sprintf("Start: %s, End: %s", rng.Start, rng.End)
}

// returns true if the given time is between [start,end]
func (rng *TimeRange) Contains(t time.Time) bool {
	return (rng.Start.Before(t) && rng.End.After(t)) || rng.Start.Equal(t) || rng.End.Equal(t)
}

type DisjointRanges struct {
	Ranges []*TimeRange
}

// merges the given range into the set of disjoint ranges.
// if the range overlaps with any of the existing ranges, the existing
// range is altered to be the union of the two ranges. Else, the given
// ranges is appended as a new disjoint range
func (dj *DisjointRanges) Merge(rng *TimeRange) {
	for i, oldrng := range dj.Ranges {
		if !rng.IsDisjoint(oldrng) {
			oldrng.MergeFrom(rng)
			dj.Ranges[i] = oldrng
			return
		}
	}
	dj.Ranges = append(dj.Ranges, rng)
}

func (dj *DisjointRanges) String() string {
	var res string
	res += fmt.Sprintln("\n┏")
	for _, rng := range dj.Ranges {
		res += fmt.Sprintln("┣", rng)
	}
	res += fmt.Sprintln("┗")
	return res
}

// takes the intersection of creation/expiry times of all DOTs on the chain
func intersectDChainAccessTimes(dchain *objects.DChain) *TimeRange {
	num := dchain.NumHashes()
	if num == 0 {
		// no hashes!
		return nil
	}
	// start with initial DOT and intersect from there
	dot := dchain.GetDOT(0)
	rng := NewTimeRange(*dot.GetCreated(), *dot.GetExpiry())
	// loop through the rest of the DOTs in the chain,
	// taking the maximum creation time and the minimum expiry time
	for i := 1; i < num; i++ {
		dot = dchain.GetDOT(i)
		if dot == nil {
			panic("Why is there a nil dot in here?")
			break
		}
		start := *dot.GetCreated()
		end := *dot.GetExpiry()
		if rng.Start.Before(start) {
			rng.Start = start
		}
		if rng.End.After(end) {
			rng.End = end
		}
	}

	return rng
}

// takes the intersection of the start/end times from URIs "archive/start/<t1>/end/<t2>/<uri>"
func intersectDChainArchivalTimes(dchain *objects.DChain) *TimeRange {
	num := dchain.NumHashes()
	if num == 0 {
		return nil
	}

	dot := dchain.GetDOT(0)
	namespace := fmtHash(dot.GetAccessURIMVK())
	start, end, _, err := parseArchiveURI(namespace + "/" + dot.GetAccessURISuffix())
	if err != nil {
		log.Println(err)
		return nil
	}
	rng := NewTimeRange(start, end)
	for i := 1; i < num; i++ {
		dot = dchain.GetDOT(i)
		if dot == nil {
			panic("Why is there a nil dot in here?")
			break
		}
		start, end, _, err := parseArchiveURI(namespace + "/" + dot.GetAccessURISuffix())
		if err != nil {
			log.Println(err)
			return nil
		}
		if rng.Start.Before(start) {
			rng.Start = start
		}
		if rng.End.After(end) {
			rng.End = end
		}
	}
	return rng
}
