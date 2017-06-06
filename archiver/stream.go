package archiver

import (
	"regexp"
	"sync"
	"time"

	"github.com/gtfierro/ob"
	"github.com/gtfierro/pundat/common"
	bw2 "github.com/immesys/bw2bind"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

var commitTick = 5 * time.Second
var commitCount = 256
var annotationTick = 5 * time.Minute

type Stream2 struct {
	// Archive request information
	subscribeURI string
	name         string
	unit         string
	po           string
	valueExpr    []ob.Operation
	timeExpr     []ob.Operation
	timeParse    string
	// uri rewriting
	urimatch   *regexp.Regexp
	urireplace string
	// incoming data
	subscription chan *bw2.SimpleMessage
	buffer       chan *bw2.SimpleMessage
	// maps URI -> UUID (under the other parameters of this archive request)
	seenURIs   map[string]common.UUID
	timeseries map[string]common.Timeseries
	sync.RWMutex
}

func (s *Stream2) initialize(timeseriesStore TimeseriesStore, metadataStore MetadataStore, msg *bw2.SimpleMessage) error {
	// don't need to worry about escaping $ in the URI because bosswave doesn't allow it
	rewrittenURI := s.urimatch.ReplaceAllString(msg.URI, s.urireplace)

	currentUUID := common.ParseUUID(uuid.NewV3(NAMESPACE_UUID, rewrittenURI+s.name).String())

	// update stream structures
	s.Lock()
	s.seenURIs[msg.URI] = currentUUID
	s.timeseries[msg.URI] = common.Timeseries{
		UUID:   currentUUID,
		SrcURI: msg.URI,
	}
	s.Unlock()

	// do initialization with the metadata store
	if metadataErr := metadataStore.InitializeURI(msg.URI, rewrittenURI, s.name, s.unit, currentUUID); metadataErr != nil {
		log.Error(errors.Wrapf(metadataErr, "Error initializing metadata store with URI %s", msg.URI))
		return metadataErr
	}

	if exists, err := timeseriesStore.StreamExists(currentUUID); err != nil {
		log.Error(errors.Wrapf(err, "Could not check stream exists (%s)", currentUUID.String()))
		return err
	} else if !exists {
		if err := timeseriesStore.RegisterStream(currentUUID, rewrittenURI, s.name, s.unit); err != nil {
			log.Error(errors.Wrapf(err, "Could not create stream (%s %s %s %s)", currentUUID.String(), msg.URI, s.name, s.unit))
			return err
		}
	}

	// start routine to push readings to the db
	go func() {
		for _ = range time.Tick(commitTick) {
			s.RLock()
			ts := s.timeseries[msg.URI]
			s.RUnlock()
			ts.Lock()
			// if no readings, then we give up
			if len(ts.Records) == 0 {
				ts.Unlock()
				continue
			}
			// now we can assume the stream exists and can write to it
			if err := timeseriesStore.AddReadings(ts); err != nil {
				log.Fatal(errors.Wrapf(err, "Could not write timeseries reading %+v", ts))
			}
			//atomic.AddInt64(&count, -1*len(ts.Records))
			ts.Records = []*common.TimeseriesReading{}
			ts.Unlock()
			s.Lock()
			s.timeseries[msg.URI] = ts
			s.Unlock()
		}
	}()

	// start goroutine to push stream metadata into timeseries store
	go func() {
		for _ = range time.Tick(annotationTick) {
			log.Infof("Updating stream annotations for %s", msg.URI)
			var uuids []common.UUID
			s.RLock()
			for _, ts := range s.timeseries {
				uuids = append(uuids, ts.UUID)
			}
			s.RUnlock()
			for _, uuid := range uuids {
				if doc := metadataStore.GetDocument(uuid); doc == nil {
					continue
				} else if err := timeseriesStore.AddAnnotations(uuid, doc); err != nil {
					log.Error(err)
				}
			}
		}
	}()

	return nil
}

func (s *Stream2) start(timeseriesStore TimeseriesStore, metadataStore MetadataStore) {
	// put messages in the local buffer
	go func() {
		for msg := range s.subscription {
			s.buffer <- msg
		}
	}()

	// loop through the buffer
	go func() {
		for msg := range s.buffer {
			if len(msg.POs) == 0 {
				continue
			}
			// if we haven't seen this URI before, then we need to initialize it in order to get the UUID
			if _, exists := s.seenURIs[msg.URI]; !exists {
				// TODO: check error?
				if err := s.initialize(timeseriesStore, metadataStore, msg); err != nil {
					log.Error(err)
					continue
				}
			}

			// grab the timeseries object
			s.RLock()
			ts := s.timeseries[msg.URI]
			s.RUnlock()
			po := msg.GetOnePODF(s.po)

			// unpack the message
			//TODO: cannot assume msgpack
			var thing interface{}
			err := po.(bw2.MsgPackPayloadObject).ValueInto(&thing)
			if err != nil {
				log.Error(errors.Wrap(err, "Could not unmarshal msgpack object"))
				continue
			}

			// extract the possible value
			value := ob.Eval(s.valueExpr, thing)
			if value == nil {
				continue
			}

			// extract the time
			timestamp := s.getTime(thing)

			// generate the timeseries values from our extracted value, and then save it
			// test if the value is a list
			if value_list, ok := value.([]interface{}); ok {
				for _, _val := range value_list {
					value_f64, ok := _val.(float64)
					if !ok {
						if value_u64, ok := value.(uint64); ok {
							value_f64 = float64(value_u64)
						} else if value_i64, ok := value.(int64); ok {
							value_f64 = float64(value_i64)
						} else if value_bool, ok := value.(bool); ok {
							if value_bool {
								value_f64 = float64(1)
							} else {
								value_f64 = float64(0)
							}
						} else {
							log.Errorf("Value %+v was not a float64 (was %T)", value, value)
							continue
						}
					}
					ts.Lock()
					ts.Records = append(ts.Records, &common.TimeseriesReading{Time: timestamp, Value: value_f64})
					ts.Unlock()
				}
			} else {
				value_f64, ok := value.(float64)
				if !ok {
					if value_u64, ok := value.(uint64); ok {
						value_f64 = float64(value_u64)
					} else if value_i64, ok := value.(int64); ok {
						value_f64 = float64(value_i64)
					} else if value_bool, ok := value.(bool); ok {
						if value_bool {
							value_f64 = float64(1)
						} else {
							value_f64 = float64(0)
						}
					} else {
						log.Errorf("Value %+v was not a float64 (was %T)", value, value)
						continue
					}
				}
				ts.Lock()
				ts.Records = append(ts.Records, &common.TimeseriesReading{Time: timestamp, Value: value_f64})
				ts.Unlock()
			}
			ts.Lock()
			if len(ts.Records) > commitCount {
				// now we can assume the stream exists and can write to it
				if err := timeseriesStore.AddReadings(ts); err != nil {
					log.Error(errors.Wrapf(err, "Could not write timeseries reading %+v", ts))
				}
				ts.Records = []*common.TimeseriesReading{}
			}
			ts.Unlock()
			s.Lock()
			s.timeseries[msg.URI] = ts
			s.Unlock()

		}
	}()
}

func (s *Stream2) getTime(thing interface{}) time.Time {
	if len(s.timeExpr) == 0 {
		return time.Now()
	}
	timeThing := ob.Eval(s.timeExpr, thing)
	timeString, ok := timeThing.(string)
	if ok {
		parsedTime, err := time.Parse(s.timeParse, timeString)
		if err != nil {
			return time.Now()
		}
		return parsedTime
	}

	timeNum, ok := timeThing.(uint64)
	if ok {
		uot := common.GuessTimeUnit(timeNum)
		i_ns, err := common.ConvertTime(timeNum, uot, common.UOT_NS)
		if err != nil {
			log.Error(err)
		}
		return time.Unix(0, int64(i_ns))
	}
	return time.Now()
}
