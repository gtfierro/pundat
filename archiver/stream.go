package archiver

import (
	"github.com/gtfierro/durandal/common"
	ob "github.com/gtfierro/giles2/objectbuilder"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"time"
)

type Stream struct {
	// timeseries identifier
	UUID     common.UUID
	uuidExpr []ob.Operation
	// immutable source of the stream. What the Archive Request points to.
	// This is what we subscribe to for data to archive (but not metadata)
	uri string
	// list of Metadata URIs
	metadata []string

	// following fields used for parsing received messages
	// the type of PO to extract
	po int
	// value expression
	valueExpr []ob.Operation
	// time expression
	timeExpr  []ob.Operation
	timeParse string

	// following fields used for operation of the stream
	cancel       chan bool
	subscription chan *bw2.SimpleMessage
	//TODO: add database reference for timeseries
}

func (s *Stream) URI() string {
	return s.uri
}

//TODO: database reference goes here
func (s *Stream) startArchiving(store TimeseriesStore) {
	ts := common.Timeseries{
		UUID:   s.UUID,
		SrcURI: s.uri,
	}
	//TODO: batching
	go func() {
		for msg := range s.subscription {
			for _, po := range msg.POs {
				if !po.IsType(s.po, s.po) {
					continue
				}
				var thing interface{}
				err := po.(bw2.MsgPackPayloadObject).ValueInto(&thing)
				if err != nil {
					log.Error(errors.Wrap(err, "Could not unmarshal msgpack object"))
					continue
				}
				value := ob.Eval(s.valueExpr, thing)
				time := s.getTime(thing)
				if len(s.UUID) == 0 {
					s.UUID = ob.Eval(s.uuidExpr, thing).(common.UUID)
				}
				log.Noticef("UUID: %v, Value: %v, time %v", s.UUID, value, time)
				ts.Records = []*common.TimeseriesReading{{Time: time, Value: value.(float64)}}
				if err := store.AddReadings(ts); err != nil {
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
