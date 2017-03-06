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

func (bdb *btrdbv4Iface) RegisterStream(streamuuid common.UUID, uri, name string) error {
	_, err := bdb.createStream(streamuuid, uri, name)
	return err
}

func (bdb *btrdbv4Iface) StreamExists(streamuuid common.UUID) (bool, error) {
	_, err := bdb.getStream(streamuuid)
	if err == nil {
		return true, nil
	} else if err == errStreamNotExist {
		return false, nil
	} else {
		return false, err
	}
}

// given a list of UUIDs, returns those for which a stream object exists
func (bdb *btrdbv4Iface) uuidsToStreams(uuids []common.UUID) []*btrdb.Stream {
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
	return streams
}

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
	streams := bdb.uuidsToStreams(uuids)
	for _, stream := range streams {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		point, generation, err := stream.Nearest(ctx, int64(start), 0, backwards)
		if err != nil {
			return results, errors.Wrapf(err, "Could not get Nearest point for %s", stream.UUID())
		}
		reading := []*common.TimeseriesReading{rawpointToTimeseriesReading(point)}
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

//func (s *Stream) RawValues(ctx context.Context, start int64, end int64, version uint64) (chan RawPoint, chan uint64, chan error)
//RawValues reads raw values from BTrDB. The returned RawPoint channel must be fully consumed.
func (bdb *btrdbv4Iface) GetData(uuids []common.UUID, start, end uint64) ([]common.Timeseries, error) {
	var results []common.Timeseries
	streams := bdb.uuidsToStreams(uuids)
	for _, stream := range streams {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		ts := common.Timeseries{
			UUID: common.ParseUUID(stream.UUID().String()),
		}
		rawpoints, generations, errchan := stream.RawValues(ctx, int64(start), int64(end), 0)
		// remember: must consume all points
		for point := range rawpoints {
			ts.Records = append(ts.Records, rawpointToTimeseriesReading(point))
		}
		ts.Generation = <-generations
		if err := <-errchan; err != nil {
			return results, errors.Wrapf(err, "Could not fetch rawdata for stream %s", stream.UUID())
		}

		results = append(results, ts)
	}
	return results, nil
}

// AlignedWindows reads power-of-two aligned windows from BTrDB.
// It is faster than Windows(). Each returned window will be 2^pointwidth nanoseconds long, starting at start.
// Note that start is inclusive, but end is exclusive.
// That is, results will be returned for all windows that start in the interval [start, end).
// If end < start+2^pointwidth you will not get any results.
// If start and end are not powers of two, the bottom pointwidth bits will be cleared.
// Each window will contain statistical summaries of the window. Statistical points with count == 0 will be omitted.
func (bdb *btrdbv4Iface) StatisticalData(uuids []common.UUID, pointWidth int, start, end uint64) ([]common.StatisticTimeseries, error) {
	var results []common.StatisticTimeseries
	streams := bdb.uuidsToStreams(uuids)
	for _, stream := range streams {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ts := common.StatisticTimeseries{
			UUID: common.ParseUUID(stream.UUID().String()),
		}
		statpoints, generations, errchan := stream.AlignedWindows(ctx, int64(start), int64(end), uint8(pointWidth), 0)
		// remember: must consume all points
		for point := range statpoints {
			ts.Records = append(ts.Records, statpointToStatisticsReading(point))
		}
		ts.Generation = <-generations
		if err := <-errchan; err != nil {
			return results, errors.Wrapf(err, "Could not fetch statdata for stream %s", stream.UUID())
		}

		results = append(results, ts)
	}
	return results, nil
}

// Windows returns arbitrary precision windows from BTrDB. It is slower than AlignedWindows, but still significantly faster than RawValues.
// Each returned window will be width nanoseconds long. start is inclusive, but end is exclusive (e.g if end < start+width you will get no results).
// That is, results will be returned for all windows that start at a time less than the end timestamp.
// If (end - start) is not a multiple of width, then end will be decreased to the greatest value less than end such that (end - start) is a multiple of width
// (i.e., we set end = start + width * floordiv(end - start, width).
// The depth parameter is an optimization that can be used to speed up queries on fast queries.
// Each window will be accurate to 2^depth nanoseconds. If depth is zero, the results are accurate to the nanosecond.
// On a dense stream for large windows, this accuracy may not be required. For example for a window of a day, +- one second may be appropriate, so a depth of 30 can be specified.
// This is much faster to execute on the database side. The StatPoint channel MUST be fully consumed.
func (bdb *btrdbv4Iface) WindowData(uuids []common.UUID, width, start, end uint64) ([]common.StatisticTimeseries, error) {
	var results []common.StatisticTimeseries
	streams := bdb.uuidsToStreams(uuids)
	for _, stream := range streams {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ts := common.StatisticTimeseries{
			UUID: common.ParseUUID(stream.UUID().String()),
		}
		statpoints, generations, errchan := stream.Windows(ctx, int64(start), int64(end), width, 0, 0)
		// remember: must consume all points
		for point := range statpoints {
			ts.Records = append(ts.Records, statpointToStatisticsReading(point))
		}
		ts.Generation = <-generations
		if err := <-errchan; err != nil {
			return results, errors.Wrapf(err, "Could not fetch statdata for stream %s", stream.UUID())
		}

		results = append(results, ts)
	}
	return results, nil
}

// func (s *Stream) Changes(ctx context.Context, fromVersion uint64, toVersion uint64, resolution uint8) (crv chan ChangedRange, cver chan uint64, cerr chan error)
func (bdb *btrdbv4Iface) ChangedRanges(uuids []common.UUID, from_gen, to_gen uint64, resolution uint8) ([]common.ChangedRange, error) {
	var results []common.ChangedRange
	streams := bdb.uuidsToStreams(uuids)
	for _, stream := range streams {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		cr := common.ChangedRange{
			UUID: common.ParseUUID(stream.UUID().String()),
		}
		changed, _, errchan := stream.Changes(ctx, from_gen, to_gen, resolution)
		for point := range changed {
			cr.Ranges = append(cr.Ranges, &common.TimeRange{Generation: point.Version, StartTime: point.Start, EndTime: point.End})
		}
		if err := <-errchan; err != nil {
			return results, errors.Wrapf(err, "Could not fetch changed ranges for stream %s", stream.UUID())
		}
		results = append(results, cr)
	}
	return results, nil
}

func (bdb *btrdbv4Iface) DeleteData(uuids []common.UUID, start, end uint64) error {
	streams := bdb.uuidsToStreams(uuids)
	for _, stream := range streams {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if _, err := stream.DeleteRange(ctx, int64(start), int64(end)); err != nil {
			return errors.Wrapf(err, "Could not delete range for stream %s", stream.UUID())
		}
	}
	return nil
}

func (bdb *btrdbv4Iface) ValidTimestamp(time uint64, uot common.UnitOfTime) bool {
	var err error
	if uot != common.UOT_NS {
		time, err = common.ConvertTime(time, uot, common.UOT_NS)
	}
	return time >= 0 && time <= MaximumTime && err == nil
}

func rawpointToTimeseriesReading(point btrdb.RawPoint) *common.TimeseriesReading {
	return &common.TimeseriesReading{Time: time.Unix(0, point.Time), Unit: common.UOT_NS, Value: point.Value}
}
func statpointToStatisticsReading(point btrdb.StatPoint) *common.StatisticsReading {
	return &common.StatisticsReading{Time: time.Unix(0, point.Time), Unit: common.UOT_NS, Min: point.Min, Mean: point.Mean, Max: point.Max, Count: point.Count}
}
