package bw2bind

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Reregister interfaces/services every ten seconds
const RegistrationInterval = 10

func handleErr(err error) {
	if err != nil {
		fmt.Println("Service encountered error: ", err)
		os.Exit(1)
	}
}

type Service struct {
	cl      *BW2Client
	name    string
	baseuri string
	ifaces  []*Interface
	mu      *sync.Mutex
}

type Interface struct {
	svc    *Service
	prefix string
	name   string
	auto   bool
	last   time.Time
}

func (cl *BW2Client) RegisterService(baseuri string, name string) *Service {
	baseuri = strings.TrimSuffix(baseuri, "/")
	rv := &Service{cl: cl, baseuri: baseuri, name: name, mu: &sync.Mutex{}}
	go rv.registerLoop()
	return rv
}

func (s *Service) registerLoop() {
	//Initial delay is lower
	time.Sleep(1 * time.Second)
	for {
		err := s.cl.SetMetadata(s.baseuri+"/"+s.name, "lastalive", time.Now().Format(time.RFC3339Nano))
		handleErr(err)
		s.mu.Lock()
		for _, i := range s.ifaces {
			if i.auto {
				i.updateRegistration()
			}
		}
		s.mu.Unlock()
		time.Sleep(RegistrationInterval * time.Second)
	}
}

func (s *Service) FullURI() string {
	return s.baseuri + "/" + s.name
}

func (s *Service) RegisterInterface(prefix string, name string) *Interface {
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = strings.TrimPrefix(prefix, "/")
	rv := &Interface{
		svc:    s,
		prefix: prefix,
		name:   name,
		auto:   true,
	}
	s.mu.Lock()
	s.ifaces = append(s.ifaces, rv)
	s.mu.Unlock()
	return rv
}

// Registers an interface that will only publish a heartbeat when the interface
// is published on
func (s *Service) RegisterInterfaceHeartbeatOnPub(prefix string, name string) *Interface {
	prefix = strings.TrimSuffix(prefix, "/")
	prefix = strings.TrimPrefix(prefix, "/")
	rv := &Interface{
		svc:    s,
		prefix: prefix,
		name:   name,
		auto:   false,
	}
	s.mu.Lock()
	s.ifaces = append(s.ifaces, rv)
	s.mu.Unlock()
	return rv
}
func (s *Service) SetMetadata(key, val string) error {
	return s.cl.SetMetadata(s.FullURI(), key, val)
}
func (ifc *Interface) FullURI() string {
	return ifc.svc.FullURI() + "/" + ifc.prefix + "/" + ifc.name
}
func (ifc *Interface) SignalURI(signal string) string {
	return ifc.FullURI() + "/signal/" + signal
}
func (ifc *Interface) SlotURI(slot string) string {
	return ifc.FullURI() + "/slot/" + slot
}
func (ifc *Interface) SetMetadata(key, val string) error {
	return ifc.svc.cl.SetMetadata(ifc.FullURI(), key, val)
}
func (ifc *Interface) GetMetadataKey(key string) (string, error) {
	dat, _, err := ifc.svc.cl.GetMetadataKey(ifc.FullURI(), key)
	return dat.Value, err
}
func (ifc *Interface) updateRegistration() {
	err := ifc.SetMetadata("lastalive", time.Now().Format(time.RFC3339Nano))
	handleErr(err)
}
func (ifc *Interface) PublishSignal(signal string, poz ...PayloadObject) error {
	if !ifc.auto && time.Now().Sub(ifc.last) > RegistrationInterval*time.Second {
		ifc.updateRegistration()
		ifc.last = time.Now()
	}
	return ifc.svc.cl.Publish(&PublishParams{
		URI:            ifc.SignalURI(signal),
		AutoChain:      true,
		PayloadObjects: poz,
		Persist:        true,
	})
}
func (ifc *Interface) PublishSignalReliable(signal string, poz ...PayloadObject) error {
	if !ifc.auto && time.Now().Sub(ifc.last) > RegistrationInterval*time.Second {
		ifc.updateRegistration()
		ifc.last = time.Now()
	}
	return ifc.svc.cl.Publish(&PublishParams{
		URI:            ifc.SignalURI(signal),
		AutoChain:      true,
		PayloadObjects: poz,
		Persist:        true,
		EnsureDelivery: true,
	})
}
func (ifc *Interface) SubscribeSlot(slot string, cb func(*SimpleMessage)) {
	rc := ifc.svc.cl.SubscribeOrExit(&SubscribeParams{
		URI:       ifc.SlotURI(slot),
		AutoChain: true,
	})
	go func() {
		for sm := range rc {
			cb(sm)
		}
	}()
}
