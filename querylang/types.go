package querylang

import (
	"github.com/gtfierro/pundat/common"
	"time"
)

type DataQuery struct {
	Dtype           DataQueryType
	Start           time.Time
	End             time.Time
	Limit           Limit
	Timeconv        common.UnitOfTime
	IsStatistical   bool
	IsWindow        bool
	IsChangedRanges bool
	FromGen         uint64
	ToGen           uint64
	Resolution      uint8
	Width           uint64
	PointWidth      int64
}

type Limit struct {
	Limit       int64
	Streamlimit int64
}

type DataQueryType uint

const (
	IN_TYPE DataQueryType = iota
	BEFORE_TYPE
	AFTER_TYPE
	CHANGED_TYPE
)

func (dt DataQueryType) String() string {
	ret := ""
	switch dt {
	case IN_TYPE:
		ret = "in"
	case BEFORE_TYPE:
		ret = "before"
	case AFTER_TYPE:
		ret = "after"
	case CHANGED_TYPE:
		ret = "changed"
	}
	return ret
}
