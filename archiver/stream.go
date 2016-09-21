package archiver

import (
	"github.com/gtfierro/durandal/common"
	"github.com/gtfierro/ob"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"time"
)

type Stream struct {
	// timeseries identifier
	//UUID     common.UUID
	uuidExpr []ob.Operation
	// immutable source of the stream. What the Archive Request points to.
	// This is what we subscribe to for data to archive (but not metadata)
	uri string
	// list of Metadata URIs
	metadataURIs    []string
	inheritMetadata bool

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
}

func (s *Stream) URI() string {
	return s.uri
}

func (s *Stream) startArchiving(timeseriesStore TimeseriesStore, metadataStore MetadataStore) {
	//TODO: Consider batching delivering new readings to BtrDB
	// Right now we deliver readings one by one to BtrDB. If the serialization becomes
	// a bottleneck, we should batch readings to amortize that cost
	go func() {
		// for each message we receive
		for msg := range s.subscription {
			// for each payload object in the message
			for _, po := range msg.POs {
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

				// extract the time
				time := s.getTime(thing)

				// if we have an expression to extract a UUID, we use that
				var currentUUID common.UUID
				if len(s.uuidExpr) > 0 {
					currentUUID = ob.Eval(s.uuidExpr, thing).(common.UUID)
				} else {
					// generate the UUID for this message's URI, POnum and value expression (and the name, when we have it)
					//TODO: add a name to the UUID
					currentUUID = common.ParseUUID(uuid.NewV3(NAMESPACE_UUID, msg.URI+po.GetPODotNum()+s.valueString).String())
				}
				ts := common.Timeseries{
					UUID:   currentUUID,
					SrcURI: msg.URI,
				}

				// map the metadata URIs for this particular URI to the generated UUID
				if err := pfx.AddTimeseriesURI(msg.URI); err != nil {
					log.Error(errors.Wrap(err, "Could not add timeseries URI from stream"))
					continue
				}
				if err := pfx.AddUUIDURIMapping(msg.URI, currentUUID); err != nil {
					log.Error(errors.Wrap(err, "Could not save mapping of uri to uuid"))
					continue
				}

				// generate the timeseries values from our extracted value, and then save it
				// test if the value is a list
				ts.Records = []*common.TimeseriesReading{}
				if value_list, ok := value.([]interface{}); ok {
					for _, _val := range value_list {
						value_f64, ok := _val.(float64)
						if !ok {
							log.Errorf("Value %+v was not a float64 from %+v", value)
							continue
						}
						ts.Records = append(ts.Records, &common.TimeseriesReading{Time: time, Value: value_f64})
					}
				} else {
					value_f64, ok := value.(float64)
					if !ok {
						log.Errorf("Value %+v was not a float64 from %+v", value)
						continue
					}
					ts.Records = append(ts.Records, &common.TimeseriesReading{Time: time, Value: value_f64})
				}
				if err := timeseriesStore.AddReadings(ts); err != nil {
					log.Error(errors.Wrapf(err, "Could not write timeseries reading %+v", ts))
				}
			}
		}
	}()
}

func (s *Stream) getTime(thing interface{}) time.Time {
	if len(s.timeExpr) == 0 {
		return time.Now()
	}
	timeString, ok := ob.Eval(s.timeExpr, thing).(string)
	if ok {
		parsedTime, err := time.Parse(s.timeParse, timeString)
		if err != nil {
			return time.Now()
		}
		return parsedTime
	}
	return time.Now()
}

/*
So how does this work?

The archiver subscribes to all !meta/giles tags, which contain ArchiveRequests.
The archiver pulls thse requests, parses them and turns them into Streams.
This involves:
- getting the list of uris for metadata and associating them with the stream
- save the state of the stream:
	- send the list of metadatauris to the subber
	- save the mapping of stream UUID to those URIs

*/
