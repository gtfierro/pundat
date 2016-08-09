package archiver

import (
	"github.com/gtfierro/durandal/common"
)

type MetadataStore interface {
	GetUnitOfTime(VK string, uuid common.UUID) (common.UnitOfTime, error)
	GetMetadata(VK string, tags []string, where common.Dict) (*common.MetadataGroup, error)
	GetDistinct(VK string, tag string, where common.Dict) (*common.MetadataGroup, error)

	SaveMetadata(records []*common.MetadataRecord) error

	RemoveMetadata(VK string, tags []string, where common.Dict) error
	MapURItoUUID(uri string, uuid common.UUID) error
	URItoUUID(uri string) (common.UUID, error)
}

type TimeseriesStore interface {
	AddReadings(common.Timeseries) error

	// list of UUIDs, reference time in nanoseconds
	// Retrieves data before the reference time for the given streams.
	Prev([]common.UUID, uint64) ([]common.Timeseries, error)

	// list of UUIDs, reference time in nanoseconds
	// Retrieves data after the reference time for the given streams.
	Next([]common.UUID, uint64) ([]common.Timeseries, error)

	// uuids, start time, end time (both in nanoseconds)
	GetData(uuids []common.UUID, start uint64, end uint64) ([]common.Timeseries, error)

	// pointWidth is the log of the number of records to aggregate
	StatisticalData(uuids []common.UUID, pointWidth int, start, end uint64) ([]common.StatisticTimeseries, error)

	// width in nanoseconds
	WindowData(uuids []common.UUID, width, start, end uint64) ([]common.StatisticTimeseries, error)

	// delete data
	DeleteData(uuids []common.UUID, start uint64, end uint64) error

	// returns true if the timestamp can be represented in the database
	ValidTimestamp(uint64, common.UnitOfTime) bool
}
