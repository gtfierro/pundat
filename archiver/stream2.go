package archiver

import (
	"regexp"

	"github.com/gtfierro/ob"
	"github.com/gtfierro/pundat/common"
	bw2 "github.com/immesys/bw2bind"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

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
}

func (s *Stream2) initialize(timeseriesStore TimeseriesStore, metadataStore MetadataStore, msg *bw2.SimpleMessage) error {
	currentUUID := common.ParseUUID(uuid.NewV3(NAMESPACE_UUID, msg.URI+s.name).String())
	s.seenURIs[msg.URI] = currentUUID

	// don't need to worry about escaping $ in the URI because bosswave doesn't allow it
	rewrittenURI := s.urimatch.ReplaceAllString(msg.URI, s.urireplace)

	// do initialization with the metadata store
	if metadataErr := metadataStore.InitializeURI(msg.URI, rewrittenURI, s.name, s.unit, currentUUID); metadataErr != nil {
		log.Error(errors.Wrapf(metadataErr, "Error initializing metadata store with URI %s", msg.URI))
		return metadataErr
	}

	// TODO: do initialization with the timeseries store

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
			// get the UUID for the current message
			var currentUUID common.UUID
			var exists bool
			// if we haven't seen this URI before, then we need to initialize it in order to get the UUID
			if currentUUID, exists = s.seenURIs[msg.URI]; !exists {
				// TODO: check error?
				if err := s.initialize(timeseriesStore, metadataStore, msg); err != nil {
					log.Error(err)
					continue
				}
				currentUUID = s.seenURIs[msg.URI]
			}
			_ = currentUUID
			//log.Debug(currentUUID)

		}
	}()
}
