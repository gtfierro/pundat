package archiver

import (
	"github.com/gtfierro/pundat/common"

	"gopkg.in/mgo.v2/bson"
)

type MetadataStore interface {
	GetUnitOfTime(VK string, uuid common.UUID) (common.UnitOfTime, error)
	GetMetadata(VK string, tags []string, where common.Dict) ([]common.MetadataGroup, error)
	GetDistinct(VK string, tag string, where common.Dict) ([]string, error)
	GetUUIDs(VK string, where common.Dict) ([]common.UUID, error)

	URIFromUUID(uuid common.UUID) (string, error)
	UUIDFromURI(uri string) (common.UUID, error)
	GetDocument(uuid common.UUID) bson.M
	InitializeURI(uri, rewrittenuri, name, unit string, uuid common.UUID) error
}

// Interface for timeseries database.
type TimeseriesStore interface {
	// returns true if the stream exists
	StreamExists(uuid common.UUID) (bool, error)

	// registers the stream with the timeseries database
	RegisterStream(uuid common.UUID, uri, name, unit string) error

	// writes a set of readings for a particular stream
	AddReadings(common.Timeseries) error

	// list of UUIDs, reference time in nanoseconds
	// Retrieves data before the reference time for the given streams.
	Prev([]common.UUID, int64) ([]common.Timeseries, error)

	// list of UUIDs, reference time in nanoseconds
	// Retrieves data after the reference time for the given streams.
	Next([]common.UUID, int64) ([]common.Timeseries, error)

	// uuids, start time, end time (both in nanoseconds)
	GetData(uuids []common.UUID, start int64, end int64) ([]common.Timeseries, error)
	GetDataUUID(uuid common.UUID, start int64, end int64, convert common.UnitOfTime) (common.Timeseries, error)

	// pointWidth is the log of the number of records to aggregate
	StatisticalData(uuids []common.UUID, pointWidth int, start, end int64) ([]common.StatisticTimeseries, error)
	StatisticalDataUUID(uuid common.UUID, pointWidth int, start, end int64, convert common.UnitOfTime) (common.StatisticTimeseries, error)

	// width in nanoseconds
	WindowData(uuids []common.UUID, width uint64, start, end int64) ([]common.StatisticTimeseries, error)
	WindowDataUUID(uuid common.UUID, width uint64, start, end int64, convert common.UnitOfTime) (common.StatisticTimeseries, error)

	// https://godoc.org/gopkg.in/btrdb.v3#BTrDBConnection.QueryChangedRanges
	ChangedRanges(uuids []common.UUID, from_gen, to_gen uint64, resolution uint8) ([]common.ChangedRange, error)

	// delete data
	DeleteData(uuids []common.UUID, start int64, end int64) error

	// returns true if the timestamp can be represented in the database
	ValidTimestamp(int64, common.UnitOfTime) bool

	AddAnnotations(uuid common.UUID, annotations map[string]interface{}) error

	// disconnects from database
	Disconnect() error
}
