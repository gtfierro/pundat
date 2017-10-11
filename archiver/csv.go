package archiver

import (
	"encoding/csv"
	"fmt"
	"github.com/gtfierro/pundat/common"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type ocWriter struct {
	file string
	*csv.Writer
}

func openOCWriter(file string) (*ocWriter, error) {
	oc := &ocWriter{
		file: file,
	}
	f, ferr := os.OpenFile(oc.file, os.O_APPEND|os.O_RDWR, 0755)
	if ferr != nil {
		return nil, ferr
	}
	oc.Writer = csv.NewWriter(f)
	return oc, f.Close()
}

func (oc *ocWriter) WriteAll(records [][]string) error {
	f, ferr := os.OpenFile(oc.file, os.O_APPEND|os.O_RDWR, 0755)
	if ferr != nil {
		return ferr
	}
	oc.Writer = csv.NewWriter(f)
	writeErr := oc.Writer.WriteAll(records)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr

}

type CSVDB struct {
	w map[string]*ocWriter
	sync.RWMutex
}

func NewCSVDB() *CSVDB {
	cdb := &CSVDB{
		w: make(map[string]*ocWriter),
	}

	files, err := filepath.Glob("data/*.csv")
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		w, err := openOCWriter(file)
		if err != nil {
			log.Fatal(err)
		}
		cdb.w[file] = w
	}

	return cdb
}

// returns true if the stream exists
func (cdb *CSVDB) StreamExists(uuid common.UUID) (bool, error) {
	filename := fmt.Sprintf("data/%s.csv", uuid.String())
	_, err := os.Stat(filename)
	return !os.IsNotExist(err), nil
}

// registers the stream with the timeseries database
func (cdb *CSVDB) RegisterStream(uuid common.UUID, uri, name, unit string) error {
	filename := fmt.Sprintf("data/%s.csv", uuid.String())
	log.Info("Registering CSV file for", filename)
	f, ferr := os.Create(filename)
	if ferr != nil {
		return ferr
	}
	if closeErr := f.Close(); closeErr != nil {
		return closeErr
	}
	cdb.Lock()
	w, err := openOCWriter(filename)
	cdb.w[filename] = w
	cdb.Unlock()
	return err
}

// writes a set of readings for a particular stream
func (cdb *CSVDB) AddReadings(ts common.Timeseries) error {
	log.Infof("Writing %d records for %s", len(ts.Records), ts.UUID.String())
	filename := fmt.Sprintf("data/%s.csv", ts.UUID.String())

	cdb.RLock()
	w, found := cdb.w[filename]
	cdb.RUnlock()
	if !found {
		log.Warning("not found", filename)
		return nil
	}

	var lines [][]string
	for _, rec := range ts.Records {
		var line = []string{strconv.FormatInt(rec.Time.UnixNano(), 10), strconv.FormatFloat(rec.Value, 'f', -1, 64)}
		lines = append(lines, line)
	}
	w.WriteAll(lines)
	return w.Error()
}

// list of UUIDs, reference time in nanoseconds
// Retrieves data before the reference time for the given streams.
func (cdb *CSVDB) Prev([]common.UUID, uint64) ([]common.Timeseries, error) {
	return []common.Timeseries{}, nil
}

// list of UUIDs, reference time in nanoseconds
// Retrieves data after the reference time for the given streams.
func (cdb *CSVDB) Next([]common.UUID, uint64) ([]common.Timeseries, error) {
	return []common.Timeseries{}, nil
}

// uuids, start time, end time (both in nanoseconds)
func (cdb *CSVDB) GetData(uuids []common.UUID, start uint64, end uint64) ([]common.Timeseries, error) {
	return []common.Timeseries{}, nil
}

// pointWidth is the log of the number of records to aggregate
func (cdb *CSVDB) StatisticalData(uuids []common.UUID, pointWidth int, start, end uint64) ([]common.StatisticTimeseries, error) {
	return []common.StatisticTimeseries{}, nil
}

// width in nanoseconds
func (cdb *CSVDB) WindowData(uuids []common.UUID, width, start, end uint64) ([]common.StatisticTimeseries, error) {
	return []common.StatisticTimeseries{}, nil
}

// https://godoc.org/gopkg.in/btrdb.v3#BTrDBConnection.QueryChangedRanges
func (cdb *CSVDB) ChangedRanges(uuids []common.UUID, from_gen, to_gen uint64, resolution uint8) ([]common.ChangedRange, error) {
	return []common.ChangedRange{}, nil
}

// delete data
func (cdb *CSVDB) DeleteData(uuids []common.UUID, start uint64, end uint64) error {
	return nil
}

// returns true if the timestamp can be represented in the database
func (cdb *CSVDB) ValidTimestamp(uint64, common.UnitOfTime) bool {
	return true
}

func (cdb *CSVDB) AddAnnotations(uuid common.UUID, annotations map[string]interface{}) error {
	return nil
}

func (cdb *CSVDB) Disconnect() error {
	cdb.Lock()
	defer cdb.Unlock()
	var err error
	for _, writer := range cdb.w {
		log.Info("Flushing", writer.file)
		writer.Flush()
		if err == nil {
			err = writer.Error()
		}
	}
	return err
}
