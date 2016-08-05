package archiver

import (
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"sync"
)

type ArchiveRequestList []*ArchiveRequest

func (arl *ArchiveRequestList) AddRequest(req *ArchiveRequest) {
	for _, old := range *arl {
		if req.Equals(old) {
			return
		}
	}
	*arl = append(*arl, req)
}

func (arl *ArchiveRequestList) Contains(req *ArchiveRequest) bool {
	for _, old := range *arl {
		if req.Equals(old) {
			return true
		}
	}
	return false
}

func (arl *ArchiveRequestList) RemoveRequest(req *ArchiveRequest) {
	for i, old := range *arl {
		if req.Equals(old) {
			(*arl)[i] = nil
			*arl = append((*arl)[:i], (*arl)[i+1:]...)
			return
		}
	}
}

type SynchronizedArchiveRequestMap struct {
	values map[string]*ArchiveRequestList
	sync.RWMutex
}

func NewSynchronizedArchiveRequestMap() *SynchronizedArchiveRequestMap {
	return &SynchronizedArchiveRequestMap{
		values: make(map[string]*ArchiveRequestList),
	}
}

func (m *SynchronizedArchiveRequestMap) Get(uri string) *ArchiveRequestList {
	m.RLock()
	defer m.RUnlock()
	return m.values[uri]
}

func (m *SynchronizedArchiveRequestMap) Set(uri string, req *ArchiveRequest) {
	m.Lock()
	defer m.Unlock()
	if list, found := m.values[uri]; !found {
		list = new(ArchiveRequestList)
		list.AddRequest(req)
	} else {
		list.AddRequest(req)
	}
}

func (m *SynchronizedArchiveRequestMap) SetList(uri string, req *ArchiveRequestList) {
	m.Lock()
	defer m.Unlock()
	if list, found := m.values[uri]; !found {
		m.values[uri] = req
	} else {
		for _, r := range *req {
			list.AddRequest(r)
		}
	}
}

func (m *SynchronizedArchiveRequestMap) Del(uri string) {
	m.Lock()
	defer m.Unlock()
	delete(m.values, uri)
}

func (m *SynchronizedArchiveRequestMap) RemoveEntry(uri string, req *ArchiveRequest) {
	m.Lock()
	defer m.Unlock()
	if list, found := m.values[uri]; found {
		list.RemoveRequest(req)
	}
}

func compareStringSliceAsSet(s1, s2 []string) bool {
	var (
		found bool
	)

	if len(s1) != len(s2) {
		return false
	}

	for _, val1 := range s1 {
		found = false
		for _, val2 := range s2 {
			if val1 == val2 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

type SubscriberMultiplexer struct {
	subs   map[string][]chan *bw2.SimpleMessage
	client *bw2.BW2Client
	sync.RWMutex
}

func NewSubscriberMultiplexer(client *bw2.BW2Client) *SubscriberMultiplexer {
	return &SubscriberMultiplexer{
		subs:   make(map[string][]chan *bw2.SimpleMessage),
		client: client,
	}
}

func (ns *SubscriberMultiplexer) AddSubscription(uri string) (chan *bw2.SimpleMessage, error) {
	ns.RLock()
	_, found := ns.subs[uri]
	ns.RUnlock()
	if !found {
		sub, err := ns.client.Subscribe(&bw2.SubscribeParams{
			URI: uri,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "Could not subscribe to %s", uri)
		}
		ns.handleSubscription(uri, sub)
		ns.Lock()
		ns.subs[uri] = []chan *bw2.SimpleMessage{}
		ns.Unlock()
	}
	ret := make(chan *bw2.SimpleMessage)
	ns.Lock()
	ns.subs[uri] = append(ns.subs[uri], ret)
	ns.Unlock()
	return ret, nil
}

func (ns *SubscriberMultiplexer) handleSubscription(uri string, sub chan *bw2.SimpleMessage) {
	go func() {
		for msg := range sub {
			ns.RLock()
			sublist := ns.subs[uri]
			ns.RUnlock()
			for i, c := range sublist {
				select {
				case c <- msg:
				default:
					// remove i from list
					sublist = append(sublist[:i], sublist[i+1:]...)
					ns.Lock()
					ns.subs[uri] = sublist
					ns.Unlock()
				}
			}
		}
	}()
}

/*
What's the API for this?
AddSubscription(uri string) chan simplemsg
*/
