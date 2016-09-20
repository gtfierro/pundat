package archiver

import (
	btrdb "github.com/SoftwareDefinedBuildings/btrdb-go"
	"github.com/gtfierro/durandal/common"
	uuidlib "github.com/pborman/uuid"
	"github.com/pkg/errors"
	"math/rand"
	"net"
	"sync"
	"time"
)

type btrdbConfig struct {
	address *net.TCPAddr
}

var BtrDBReadErr = errors.New("Error receiving data from BtrDB")

const MaximumTime = (48 << 56)

type btrIface struct {
	address *net.TCPAddr
	client  *btrdb.BTrDBConnection
	clients []*btrdb.BTrDBConnection
	sync.RWMutex
}

func newBtrIface(c *btrdbConfig) *btrIface {
	rand.Seed(time.Now().UnixNano())
	var err error
	b := &btrIface{
		address: c.address,
		clients: make([]*btrdb.BTrDBConnection, 10),
	}
	log.Noticef("Connecting to BtrDB at %v...", b.address.String())

	if b.client, err = btrdb.NewBTrDBConnection(c.address.String()); err != nil {
		log.Fatalf("Could not connect to btrdb: %v", err)
	}

	for i := 0; i < 10; i++ {
		c, err := btrdb.NewBTrDBConnection(c.address.String())
		if err != nil {
			log.Fatalf("Could not connect to btrdb: %v", err)
		}
		b.clients[i] = c
	}

	return b
}

func (bdb *btrIface) getClient() *btrdb.BTrDBConnection {
	bdb.RLock()
	defer bdb.RUnlock()
	return bdb.clients[rand.Intn(10)]
}

func (bdb *btrIface) AddReadings(ts common.Timeseries) error {
	var (
		parsed_uuid uuidlib.UUID
		err         error
	)

	// turn the string representation into UUID bytes
	parsed_uuid = uuidlib.UUID(ts.UUID)

	records := make([]btrdb.StandardValue, len(ts.Records))
	for i, rdg := range ts.Records {
		records[i] = btrdb.StandardValue{Time: rdg.Time.UnixNano(), Value: rdg.Value}
	}
	client := bdb.getClient()
	c, err := client.InsertValues(parsed_uuid, records, false)
	<-c // wait for response
	return err
}

func (bdb *btrIface) numberResponseFromChan(c chan btrdb.StandardValue) common.Timeseries {
	var sr = common.Timeseries{
		Records: []*common.TimeseriesReading{},
	}
	for val := range c {
		sr.Records = append(sr.Records, &common.TimeseriesReading{Time: time.Unix(0, val.Time), Value: val.Value})
	}
	return sr
}

func (bdb *btrIface) statisticalResponseFromChan(c chan btrdb.StatisticalValue) common.StatisticTimeseries {
	var sr = common.StatisticTimeseries{
		Records: []*common.StatisticsReading{},
	}
	for val := range c {
		sr.Records = append(sr.Records, &common.StatisticsReading{Time: time.Unix(0, val.Time), Count: val.Count, Min: val.Min, Max: val.Max, Mean: val.Mean})
	}
	return sr
}

func (bdb *btrIface) queryNearestValue(uuids []common.UUID, start uint64, backwards bool) ([]common.Timeseries, error) {
	var ret = make([]common.Timeseries, len(uuids))
	var results []chan btrdb.StandardValue
	client := bdb.getClient()
	for _, uu := range uuids {
		uuid := uuidlib.UUID(uu)
		values, _, _, err := client.QueryNearestValue(uuid, int64(start), backwards, 0)
		if err != nil {
			return ret, err
		}
		results = append(results, values)
	}
	for i, c := range results {
		sr := bdb.numberResponseFromChan(c)
		sr.UUID = uuids[i]
		ret[i] = sr
	}
	return ret, nil
}

func (bdb *btrIface) Prev(uuids []common.UUID, start uint64) ([]common.Timeseries, error) {
	return bdb.queryNearestValue(uuids, start, true)
}

func (bdb *btrIface) Next(uuids []common.UUID, start uint64) ([]common.Timeseries, error) {
	return bdb.queryNearestValue(uuids, start, false)
}

func (bdb *btrIface) GetData(uuids []common.UUID, start, end uint64) ([]common.Timeseries, error) {
	var ret = make([]common.Timeseries, len(uuids))
	var results []chan btrdb.StandardValue
	client := bdb.getClient()
	for _, uu := range uuids {
		uuid := uuidlib.UUID(uu)
		values, _, _, err := client.QueryStandardValues(uuid, int64(start), int64(end), 0)
		if err != nil {
			return ret, err
		}
		results = append(results, values)
	}
	for i, c := range results {
		sr := bdb.numberResponseFromChan(c)
		sr.UUID = uuids[i]
		ret[i] = sr
	}
	return ret, nil
}

func (bdb *btrIface) StatisticalData(uuids []common.UUID, pointWidth int, start, end uint64) ([]common.StatisticTimeseries, error) {
	var ret = make([]common.StatisticTimeseries, len(uuids))
	var results []chan btrdb.StatisticalValue
	client := bdb.getClient()
	for _, uu := range uuids {
		uuid := uuidlib.Parse(string(uu))
		values, _, _, err := client.QueryStatisticalValues(uuid, int64(start), int64(end), uint8(pointWidth), 0)
		if err != nil {
			return ret, err
		}
		results = append(results, values)
	}
	for i, c := range results {
		sr := bdb.statisticalResponseFromChan(c)
		sr.UUID = uuids[i]
		ret[i] = sr
	}
	return ret, nil
}

func (bdb *btrIface) WindowData(uuids []common.UUID, width, start, end uint64) ([]common.StatisticTimeseries, error) {
	var ret = make([]common.StatisticTimeseries, len(uuids))
	var results []chan btrdb.StatisticalValue
	client := bdb.getClient()
	for _, uu := range uuids {
		uuid := uuidlib.Parse(string(uu))
		values, _, _, err := client.QueryWindowValues(uuid, int64(start), int64(end), width, 0, 0)
		if err != nil {
			return ret, err
		}
		results = append(results, values)
	}
	for i, c := range results {
		sr := bdb.statisticalResponseFromChan(c)
		sr.UUID = uuids[i]
		ret[i] = sr
	}
	return ret, nil
}

func (bdb *btrIface) DeleteData(uuids []common.UUID, start uint64, end uint64) error {
	client := bdb.getClient()
	for _, uu := range uuids {
		uuid := uuidlib.Parse(string(uu))
		if _, err := client.DeleteValues(uuid, int64(start), int64(end)); err != nil {
			return err
		}
	}
	return nil
}

func (bdb *btrIface) ValidTimestamp(time uint64, uot common.UnitOfTime) bool {
	var err error
	if uot != common.UOT_NS {
		time, err = common.ConvertTime(time, uot, common.UOT_NS)
	}
	return time >= 0 && time <= MaximumTime && err == nil
}
