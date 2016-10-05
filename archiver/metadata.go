package archiver

import (
	"github.com/gtfierro/durandal/common"
	"github.com/gtfierro/durandal/prefix"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"sync"
	"time"
)

type subscription struct {
	// the uri this subscribes to
	uri string
	// the number of streams subscribed to this stream
	refs uint64
	// subscription
	sub chan *bw2.SimpleMessage
	// last value of this metadata subscription
	lastValue *common.MetadataRecord
	cancel    chan bool
	sync.Mutex
}

func newSubscription(uri string, sub chan *bw2.SimpleMessage) *subscription {
	s := &subscription{
		uri:       uri,
		refs:      1,
		lastValue: nil,
		sub:       sub,
		cancel:    make(chan bool, 1),
	}
	return s
}

func (sub *subscription) add() {
	sub.Lock()
	sub.refs += 1
	sub.Unlock()
}

func (sub *subscription) dec() {
	sub.Lock()
	sub.refs -= 1
	if sub.refs == 0 {
		sub.cancel <- true
	}
	sub.Unlock()
}

// Handles subscribing to all of the URIs for metadata.
type metadatasubscriber struct {
	client        *bw2.BW2Client
	store         MetadataStore
	pfx           *prefix.PrefixStore
	subscriptions map[string]*subscription
	uncommitted   []*common.MetadataRecord
	commitLock    sync.Mutex
	commitTimer   *time.Ticker
	sync.RWMutex
}

func newMetadataSubscriber(client *bw2.BW2Client, store MetadataStore, pfx *prefix.PrefixStore) *metadatasubscriber {
	ms := &metadatasubscriber{
		client:        client,
		store:         store,
		pfx:           pfx,
		subscriptions: make(map[string]*subscription),
		commitTimer:   time.NewTicker(5 * time.Second),
	}
	go func() {
		for _ = range ms.commitTimer.C {
			ms.commitLock.Lock()
			if err := ms.store.SaveMetadata(ms.uncommitted); err != nil {
				log.Error(errors.Wrap(err, "Could not save metadata"))
			}
			ms.uncommitted = []*common.MetadataRecord{}
			ms.commitLock.Unlock()
		}
	}()
	return ms
}

func (ms *metadatasubscriber) requestSubscription(uri string) {
	ms.Lock()
	defer ms.Unlock()
	// if the subscription already exists, return early
	if ms.subscriptions[uri] != nil {
		ms.subscriptions[uri].add()
		return
	}
	sub, err := ms.client.Subscribe(&bw2.SubscribeParams{
		URI: uri,
	})
	if err != nil {
		log.Error(errors.Wrapf(err, "Could not subscribe to %s", uri))
		return
	}
	s := newSubscription(uri, sub)
	ms.subscriptions[uri] = s
	go func(s *subscription) {
		for {
			select {
			case msg := <-s.sub:
				rec := common.RecordFromMessage(msg)
				if rec == nil {
					continue
				}
				if err := ms.pfx.AddMetadataURI(msg.URI); err != nil {
					log.Error(errors.Wrap(err, "Could not save MetadataURI"))
				}
				s.lastValue = rec
				ms.commitLock.Lock()
				ms.uncommitted = append(ms.uncommitted, rec)
				ms.commitLock.Unlock()
			case <-s.cancel:
				close(s.sub)
				return
			}
		}
	}(s)

	// run query
	query, err := ms.client.Query(&bw2.QueryParams{
		URI: uri,
	})
	if err != nil {
		log.Error(errors.Wrapf(err, "Could not query %s", uri))
		return
	}
	// this will drain and return
	for msg := range query {
		sub <- msg
	}
}
