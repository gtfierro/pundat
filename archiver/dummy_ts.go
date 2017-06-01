package archiver

import (
	"github.com/gtfierro/pundat/common"
)

type dummyts struct {
}

func (ts *dummyts) StreamExists(uuid common.UUID) (bool, error) {
	return true, nil
}
func (ts *dummyts) RegisterStream(uuid common.UUID, uri, name string) error {
	return nil
}
func (ts *dummyts) AddReadings(common.Timeseries) error {
	return nil
}
func (ts *dummyts) Prev([]common.UUID, uint64) ([]common.Timeseries, error) {
	return []common.Timeseries{}, nil
}
func (ts *dummyts) Next([]common.UUID, uint64) ([]common.Timeseries, error) {
	return []common.Timeseries{}, nil
}
func (ts *dummyts) GetData(uuids []common.UUID, start uint64, end uint64) ([]common.Timeseries, error) {
	return []common.Timeseries{}, nil
}
func (ts *dummyts) StatisticalData(uuids []common.UUID, pointWidth int, start, end uint64) ([]common.StatisticTimeseries, error) {
	return []common.StatisticTimeseries{}, nil
}
func (ts *dummyts) WindowData(uuids []common.UUID, width, start, end uint64) ([]common.StatisticTimeseries, error) {
	return []common.StatisticTimeseries{}, nil
}
func (ts *dummyts) ChangedRanges(uuids []common.UUID, from_gen, to_gen uint64, resolution uint8) ([]common.ChangedRange, error) {
	return []common.ChangedRange{}, nil
}
func (ts *dummyts) DeleteData(uuids []common.UUID, start uint64, end uint64) error {
	return nil
}
func (ts *dummyts) ValidTimestamp(uint64, common.UnitOfTime) bool {
	return true
}
