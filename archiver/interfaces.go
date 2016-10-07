package archiver

import (
	"github.com/gtfierro/pundat/common"
)

type MetadataStore interface {
	GetUnitOfTime(VK string, uuid common.UUID) (common.UnitOfTime, error)
	GetMetadata(VK string, tags []string, where common.Dict) ([]common.MetadataGroup, error)
	GetDistinct(VK string, tag string, where common.Dict) ([]string, error)
	GetUUIDs(VK string, where common.Dict) ([]common.UUID, error)

	SaveMetadata(records []*common.MetadataRecord) error

	RemoveMetadata(VK string, tags []string, where common.Dict) error
	MapURItoUUID(uri string, uuid common.UUID) error
	URItoUUID(uri string) (common.UUID, error)
	URIFromUUID(uuid common.UUID) (string, error)
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

	// https://godoc.org/gopkg.in/btrdb.v3#BTrDBConnection.QueryChangedRanges
	ChangedRanges(uuids []common.UUID, from_gen, to_gen uint64, resolution uint8) ([]common.ChangedRange, error)

	// delete data
	DeleteData(uuids []common.UUID, start uint64, end uint64) error

	// returns true if the timestamp can be represented in the database
	ValidTimestamp(uint64, common.UnitOfTime) bool
}
