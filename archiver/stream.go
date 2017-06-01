package archiver

import (
	"sync"
	"time"

	"github.com/gtfierro/ob"
	"github.com/gtfierro/pundat/common"
	bw2 "github.com/immesys/bw2bind"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

type Stream struct {
	// timeseries identifier
	//UUID     common.UUID
	uuidExpr []ob.Operation
	// immutable source of the stream. What the Archive Request points to.
	// This is what we subscribe to for data to archive (but not metadata)
	uri  string
	name string
	// list of Metadata URIs
	metadataURIs []string

	// following fields used for parsing received messages
	// the type of PO to extract
	po int
	// value expression
	valueExpr   []ob.Operation
	valueString string
	// time expression
	timeExpr  []ob.Operation
	timeParse string

	// following fields used for operation of the stream
	cancel       chan bool
	subscription chan *bw2.SimpleMessage
	buffer       chan *bw2.SimpleMessage
	// maps URI -> UUID (under the other parameters of this archive request)
	seenURIs   map[string]common.UUID
	timeseries map[string]common.Timeseries
	sync.RWMutex
}

func (s *Stream) URI() string {
	return s.uri
}

// when we see a URI for the first time, we need to initialize it:
// - get the UUID
// - metadatastore.MapURItoUUID
// - metadatastore.AddNameTag
// - create the necessary data structures in seenURIs, timeseries
func (s *Stream) initialize(metadataStore MetadataStore, timeseriesStore TimeseriesStore, msg *bw2.SimpleMessage) error {
	po := msg.POs[0]
	// skip if its not the PO we expect
	if !po.IsType(s.po, s.po) {
		return nil
	}

	// unpack the message
	//TODO: cannot assume msgpack
	var thing interface{}
	err := po.(bw2.MsgPackPayloadObject).ValueInto(&thing)
	if err != nil {
		return errors.Wrap(err, "Could not unmarshal msgpack object")
	}
	// if we have an expression to extract a UUID, we use that
	var currentUUID common.UUID
	if len(s.uuidExpr) > 0 {
		currentUUID = ob.Eval(s.uuidExpr, thing).(common.UUID)
	} else {
		// generate the UUID for this message's URI, POnum and value expression (and the name, when we have it)
		currentUUID = common.ParseUUID(uuid.NewV3(NAMESPACE_UUID, msg.URI+po.GetPODotNum()+s.name).String())
	}
	//	When we observe a UUID, we need to build up the associations to its metadata
	//	When I get a new UUID, with a URI, I need to find all of the Metadata rcords
	//	in the MD database that are prefixes of this URI (w/o !meta suffix) and add
	//	those associations in when we need to
	//if err := metadataStore.MapURItoUUID(msg.URI, currentUUID); err != nil {
	//	return err
	//}

	log.Debug("INITIALIZE", msg.URI, s.name, currentUUID)
	//if err := metadataStore.AddNameTag(s.name, currentUUID); err != nil {
	//	return err
	//}

	s.Lock()
	// add to the local maps
	s.seenURIs[msg.URI] = currentUUID
	s.timeseries[msg.URI] = common.Timeseries{
		UUID:   currentUUID,
		SrcURI: msg.URI,
	}
	s.Unlock()

	// start go routine to push readings to the db
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

	return nil
}

//
func (s *Stream) startArchiving(timeseriesStore TimeseriesStore, metadataStore MetadataStore) {
	go func() {
		for msg := range s.subscription {
			s.buffer <- msg
		}
	}()
	go func() {
		// for each message we receive
		for msg := range s.buffer {
			if len(msg.POs) == 0 {
				continue
			}
			// for each payload object in the message
			var currentUUID common.UUID
			var exists bool
			if currentUUID, exists = s.seenURIs[msg.URI]; !exists {
				// TODO: check error?
				if err := s.initialize(metadataStore, timeseriesStore, msg); err != nil {
					log.Error(err)
					continue
				}
				currentUUID = s.seenURIs[msg.URI]
			}
			// grab the timeseries object
			s.RLock()
			ts := s.timeseries[msg.URI]
			s.RUnlock()
			po := msg.POs[0]
			// skip if its not the PO we expect
			if !po.IsType(s.po, s.po) {
				continue
			}

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
					} else {
						log.Errorf("Value %+v was not a float64 (was %T)", value, value)
						continue
					}
				}
				ts.Lock()
				ts.Records = append(ts.Records, &common.TimeseriesReading{Time: timestamp, Value: value_f64})
				ts.Unlock()
			}

			// We will check the cache first (using new interface call into btrdb)
			// and create the stream object if it doesn't exist.
			if exists, err := timeseriesStore.StreamExists(currentUUID); err != nil {
				log.Error(errors.Wrapf(err, "Could not check stream exists (%s)", currentUUID.String()))
				continue
			} else if !exists {
				//if err := timeseriesStore.RegisterStream(currentUUID, msg.URI, s.name); err != nil {
				//	log.Error(errors.Wrapf(err, "Could not create stream (%s %s %s)", currentUUID.String(), msg.URI, s.name))
				//	continue
				//}
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

func (s *Stream) getTime(thing interface{}) time.Time {
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
