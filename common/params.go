package common

import (
	"fmt"
)

type QueryParams interface {
	Dump() string
}

type TagParams struct {
	Tags  []string
	Where Dict
}

func (params TagParams) Dump() string {
	ret := fmt.Sprintf("SELECT\nTags:\n")
	for _, tag := range params.Tags {
		ret += fmt.Sprintf("-> %s\n", tag)
	}
	ret += fmt.Sprintf("WHERE\n%+v", params.Where)
	return ret
}

type DistinctParams struct {
	Tag   string
	Where Dict
}

func (params DistinctParams) Dump() string {
	ret := fmt.Sprintf("SELECT DISTINCT\nTag: %s\n", params.Tag)
	ret += fmt.Sprintf("WHERE\n%+v", params.Where)
	return ret
}

// 3 valid states:
// - !IsStatistical && !IsWindow: normal range query
// - !IsStatistical && IsWindow: window query
// - IsStatistical && !IsWindow: statistical query
// - IsStatistical && IsWindow: INVALID
type DataParams struct {
	// clause to evaluate for which streams to fetch.
	// If this is empty, uses the UUIDs
	Where Dict
	// UUIDs from which to fetch data. Superceded by Where
	UUIDs []UUID
	// restrict the number of streams returned
	StreamLimit int
	// restrict the number of data points per stream returned.
	// Defaults to the most recent
	DataLimit int
	// time to start fetching data from (inclusive)
	Begin int64
	// time to stop fetching data from (inclusive)
	End int64
	// converts all readings to this unit of time when finished
	ConvertToUnit UnitOfTime
	// if true, then we interpret pointwidth
	IsStatistical bool
	// PointWidth of X means the window size is (1 << X) nanoseconds
	PointWidth int
	// if true, then we interpret windowwidth.
	IsWindow bool
	// we interpret this as nanoseconds
	Width           uint64
	IsChangedRanges bool
	FromGen         uint64
	ToGen           uint64
	Resolution      uint8
}

func (params DataParams) Dump() string {
	ret := fmt.Sprintf("DATA\n%d UUIDs\nWHERE:\n%+v", len(params.UUIDs), params.Where)
	ret += fmt.Sprintf("Begin: %d\nEnd: %d\n", params.Begin, params.End)
	ret += fmt.Sprintf("Convert to : %s", params.ConvertToUnit.String())
	return ret
}
