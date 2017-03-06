package archiver

import (
	"context"
	"sync"
	"time"

	"github.com/gtfierro/pundat/common"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"gopkg.in/btrdb.v4"
)

var timeout = time.Second * 30

var errStreamNotExist = errors.New("Stream does not exist")

type btrdbv4Config struct {
	addresses []string
}

type btrdbv4Iface struct {
	addresses       []string
	conn            *btrdb.BTrDB
	streamCache     map[string]*btrdb.Stream
	streamCacheLock sync.RWMutex
}

func newBTrDBv4(c *btrdbv4Config) *btrdbv4Iface {
	b := &btrdbv4Iface{
		addresses:   c.addresses,
		streamCache: make(map[string]*btrdb.Stream),
	}
	log.Noticef("Connecting to BtrDBv4 at addresses %v...", b.addresses)
	conn, err := btrdb.Connect(context.Background(), b.addresses...)
	if err != nil {
		log.Fatalf("Could not connect to btrdbv4: %v", err)
	}
	b.conn = conn

	return b
}

// Fetch the stream object so we can read/write. This will first check the internal in-memory
// cache of stream objects, then it will check the BtrDB client cache. If the stream
// is not found there, then this method will return errStreamNotExist and a nil stream
func (bdb *btrdbv4Iface) getStream(streamuuid common.UUID) (stream *btrdb.Stream, err error) {
	// first check cache
	bdb.streamCacheLock.RLock()
	stream, found := bdb.streamCache[streamuuid.String()]
	bdb.streamCacheLock.RUnlock()
	if found {
		return // from cache
	}
	// then check BtrDB for existing stream
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream = bdb.conn.StreamFromUUID(uuid.Parse(streamuuid.String()))
	if exists, existsErr := stream.Exists(ctx); existsErr != nil {
		if existsErr != nil {
			err = errors.Wrap(existsErr, "Could not fetch stream")
			return
		}
	} else if exists {
		bdb.streamCacheLock.Lock()
		bdb.streamCache[streamuuid.String()] = stream
		bdb.streamCacheLock.Unlock()
		return
	}

	// else where we return a nil stream and the errStreamNotExist, which signals to the
	// caller that this stream needs to be created using bdb.createStream
	err = errStreamNotExist
	return
}

// This will create a stream object w/n BtrDB, provided it does not already exist (which
// this method will check).
// A stream in BtrDB needs:
// - a UUID (which we get from the archive request)
// - a collection (which is the URI a message was published on)
// - a set of tags (There will be one tag: name=request.Name)
func (bdb *btrdbv4Iface) createStream(streamuuid common.UUID, uri, name string) (stream *btrdb.Stream, err error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stream, err = bdb.conn.Create(ctx, uuid.Parse(streamuuid.String()), uri, map[string]string{"name": name}, nil)
	if err != nil {
		bdb.streamCacheLock.Lock()
		bdb.streamCache[streamuuid.String()] = stream
		bdb.streamCacheLock.Unlock()
	}
	return
}

// TODO: Problem here is that if the stream does not exist at this point in time, then
// we need to grab other components from the archive request in order to ensure that
// the stream is created. Where's the best place to do this? Probably in the constructor
// for the stream
func (bdb *btrdbv4Iface) AddReadings(readings common.Timeseries) error {
	// get the stream object from the cache
	stream, err := bdb.getStream(readings.UUID)
	if err != nil {
		return errors.Wrap(err, "AddReadings: could not get stream")
	}
	// func (s *Stream) InsertF(ctx context.Context, length int, time func(int) int64, val func(int) float64) error
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	timefunc := func(i int) int64 {
		return readings.Records[i].Time.UnixNano()
	}
	valfunc := func(i int) float64 {
		return readings.Records[i].Value
	}
	return stream.InsertF(ctx, len(readings.Records), timefunc, valfunc)
}

// given a list of UUIDs, return the nearst point (used for both Next and Prev calls)
// Need to filter that list of UUIDs by those that exist
func (bdb *btrdbv4Iface) nearest(uuids []common.UUID, start uint64, backwards bool) ([]common.Timeseries, error) {
	var results []common.Timeseries
	var streams []*btrdb.Stream
	// filter the list of uuids by those that are actually streams
	for _, id := range uuids {
		// grab the stream object from the cache
		stream, err := bdb.getStream(id)
		if err == nil {
			streams = append(streams, stream)
			continue
		}
		if err == errStreamNotExist {
			continue // skip if no stream
		}
		log.Error(errors.Wrapf(err, "Could not find stream %s", id))
	}

	for _, stream := range streams {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		point, generation, err := stream.Nearest(ctx, int64(start), 0, backwards)
		if err != nil {
			return results, errors.Wrapf(err, "Could not get Nearest point for %s", stream.UUID())
		}
		reading := []*common.TimeseriesReading{&common.TimeseriesReading{Time: time.Unix(0, point.Time), Unit: common.UOT_NS, Value: point.Value}}
		ts := common.Timeseries{
			Records:    reading,
			Generation: generation,
			UUID:       common.ParseUUID(stream.UUID().String()),
		}

		results = append(results, ts)
	}
	return results, nil
}

func (bdb *btrdbv4Iface) Prev(uuids []common.UUID, beforeTime uint64) ([]common.Timeseries, error) {
	return bdb.nearest(uuids, beforeTime, true)
}
func (bdb *btrdbv4Iface) Next(uuids []common.UUID, afterTime uint64) ([]common.Timeseries, error) {
	return bdb.nearest(uuids, afterTime, false)
}

//	// uuids, start time, end time (both in nanoseconds)
//	GetData(uuids []common.UUID, start uint64, end uint64) ([]common.Timeseries, error)
//
//	// pointWidth is the log of the number of records to aggregate
//	StatisticalData(uuids []common.UUID, pointWidth int, start, end uint64) ([]common.StatisticTimeseries, error)
//
//	// width in nanoseconds
//	WindowData(uuids []common.UUID, width, start, end uint64) ([]common.StatisticTimeseries, error)
//
//	// https://godoc.org/gopkg.in/btrdb.v3#BTrDBConnection.QueryChangedRanges
//	ChangedRanges(uuids []common.UUID, from_gen, to_gen uint64, resolution uint8) ([]common.ChangedRange, error)
//
//	// delete data
//	DeleteData(uuids []common.UUID, start uint64, end uint64) error
//
//	// returns true if the timestamp can be represented in the database
//	ValidTimestamp(uint64, common.UnitOfTime) bool
