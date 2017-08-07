package archiver

import (
	"context"
	"regexp"
	"strings"
	"sync"

	"github.com/gtfierro/bw2util"
	"github.com/gtfierro/ob"
	"github.com/gtfierro/pundat/common"
	"github.com/gtfierro/pundat/scraper"
	bw2 "github.com/immesys/bw2bind"
	"github.com/pkg/errors"
)

// takes care of handling/parsing archive requests
type viewManager struct {
	client     *bw2.BW2Client
	bwcfg      BWConfig
	store      MetadataStore
	ts         TimeseriesStore
	vk         string
	bw2address string
	bw2entity  string
	incoming   chan *bw2.SimpleMessage
	// map of alias -> VK namespace
	namespaceAliases map[string]string
	subscribed       map[string]struct{}
	namespaceLock    sync.Mutex
	subscriptions    map[string]*subscriptionMux
	subscriptionLock sync.RWMutex
	requestHosts     *SynchronizedArchiveRequestMap
	requestURIs      *SynchronizedArchiveRequestMap
}

func newViewManager(client *bw2.BW2Client, vk string, cfg BWConfig, store MetadataStore, ts TimeseriesStore, bw2address, bw2entity string) *viewManager {
	vm := &viewManager{
		client:           client,
		bwcfg:            cfg,
		store:            store,
		ts:               ts,
		vk:               vk,
		bw2address:       bw2address,
		bw2entity:        bw2entity,
		incoming:         make(chan *bw2.SimpleMessage, 100),
		namespaceAliases: make(map[string]string),
		subscriptions:    make(map[string]*subscriptionMux),
		subscribed:       make(map[string]struct{}),
		requestHosts:     NewSynchronizedArchiveRequestMap(),
		requestURIs:      NewSynchronizedArchiveRequestMap(),
	}
	go func() {
		for msg := range vm.incoming {
			parts := strings.Split(msg.URI, "/")
			key := parts[len(parts)-1]
			if key != "archiverequest" {
				continue
			}
			var requests []*ArchiveRequest
			for _, po := range msg.POs {
				if !po.IsTypeDF(bw2.PODFGilesArchiveRequest) {
					continue
				}
				log.Debug("get po", po.GetPODotNum(), msg.URI, po.IsTypeDF(bw2.PODFGilesArchiveRequest))
				var request = new(ArchiveRequest)
				err := po.(bw2.MsgPackPayloadObject).ValueInto(request)
				if err != nil {
					log.Error(errors.Wrap(err, "Could not parse Archive Request"))
					continue
				}
				if len(request.PO) == 0 {
					log.Error("Request contained no PO")
					continue
				}
				if len(request.Name) == 0 {
					log.Error("Request contained no Name")
					continue
				}
				if len(request.ValueExpr) == 0 {
					log.Error("Request contained no Value expression")
					continue
				}
				if len(request.URI) == 0 {
					log.Error("Request contained no URI")
					continue
				}
				request.FromVK = msg.From
				chain, err := vm.client.BuildAnyChain(request.URI, "C", request.FromVK)
				if err != nil || chain == nil {
					log.Error(errors.Wrapf(err, "VK %s did not have permission to archive %s", request.FromVK, request.URI))
					continue
				}
				requests = append(requests, request)
			}
			for _, request := range requests {
				ctx := context.TODO()
				if err := vm.HandleArchiveRequest(ctx, request); err != nil {
					log.Error(errors.Wrapf(err, "Could not handle archive request %+v", request))
				}
			}
		}
	}()
	return vm
}

// Given a namespace, we subscribe to <ns>/*/!meta/archiverequest. For each received message
// on the URI, we extract the list of ArchiveRequests
func (vm *viewManager) subscribeNamespace(ctx context.Context, ns string) {
	vm.namespaceLock.Lock()
	defer vm.namespaceLock.Unlock()

	namespace := strings.TrimSuffix(ns, "/") + "/*/!meta/archiverequest"
	log.Noticef("Intend to subscribe to %s", namespace)

	client := bw2.ConnectOrExit(vm.bwcfg.Address)
	client.OverrideAutoChainTo(true)
	client.SetEntityFileOrExit(vm.bwcfg.Entityfile)

	c2, err := bw2util.NewClient(client, vm.vk)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Problem in creating new client"))
	}
	inp, err := c2.MultiSubscribe(ns + "/*/!meta/archiverequest")
	if err != nil {
		log.Fatal(errors.Wrap(err, "Problem in multi subscribe"))
	}
	log.Noticef("Subscribe to %s", namespace)
	log.Info("Creating new client for", ns)
	data, _, err := c2.ResolveLongAlias(ns)
	if err != nil {
		log.Error(err)
		return
	}
	resolved_ns := bw2.ToBase64(data)
	vm.namespaceAliases[ns] = resolved_ns
	go func() {
		for {
			select {
			case msg := <-inp:
				vm.incoming <- msg
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (vm *viewManager) HandleArchiveRequest(ctx context.Context, request *ArchiveRequest) error {
	//TODO: need a mapping from the archive requests to the URI that provided them so that we can detect when an archive request is removed
	if request.FromVK == "" {
		return errors.New("VK was empty in ArchiveRequest")
	}

	s2 := &Stream{}
	s2.buffer = make(chan *bw2.SimpleMessage, 10000)
	s2.seenURIs = make(map[string]common.UUID)
	s2.timeseries = make(map[string]common.Timeseries)
	s2.subscribeURI = request.URI
	s2.name = request.Name
	s2.unit = request.Unit
	s2.po = request.PO
	s2.valueExpr = ob.Parse(request.ValueExpr)
	if len(request.TimeExpr) > 0 {
		s2.timeExpr = ob.Parse(request.TimeExpr)
	}
	s2.timeParse = request.TimeParse
	re, err := regexp.Compile(request.URIMatch)
	if err != nil {
		log.Error(errors.Wrapf(err, "Could not parse regexp %s", request.URIMatch))
		return err
	}
	s2.urimatch = re
	s2.urireplace = request.URIReplace
	ns := strings.Split(s2.subscribeURI, "/")[0]

	// create a client for this archive request
	vm.subscriptionLock.Lock()
	var mux *subscriptionMux
	var found bool
	if mux, found = vm.subscriptions[s2.subscribeURI]; !found {
		// need to create
		var client *bw2util.Client
		_mdclient := bw2.ConnectOrExit(vm.bw2address)
		_mdclient.OverrideAutoChainTo(true)
		vk := _mdclient.SetEntityFileOrExit(vm.bw2entity)
		client, err = bw2util.NewClient(_mdclient, vk)
		if err != nil {
			log.Fatal(err)
		}

		// check if we are subscribed to metadata on this namespace
		vm.namespaceLock.Lock()
		if _, found := vm.subscribed[ns]; !found {
			l := &scraper.Listener{
				Client:    client,
				Namespace: ns,
			}
			go l.Init()
			vm.subscribed[ns] = struct{}{} // mark as subscribed
		}
		vm.namespaceLock.Unlock()

		sub, err := client.BW2Client.Subscribe(&bw2.SubscribeParams{
			URI: s2.subscribeURI,
		})
		if err != nil {
			log.Error(errors.Wrapf(err, "Could not subscribe to %s", s2.subscribeURI))
			return errors.Wrapf(err, "Could not subscribe to %s", s2.subscribeURI)
		}
		log.Info("Subscribing to", s2.subscribeURI)

		mux = newSubscriptionMux(ctx, sub)
		vm.subscriptions[s2.subscribeURI] = mux
	}
	vm.subscriptionLock.Unlock()

	mux.add(s2.buffer)

	// indicate that we've gotten an archive request
	request.Dump()

	// now, we save the stream
	s2.start(vm.ts, vm.store)

	return nil
}

// removes from the hostURI mapping all of those requests that aren't in recentRequests list
func (vm *viewManager) UpdateArchiveRequests(hostURI string, recentRequests []*ArchiveRequest) {
	var keepList = new(ArchiveRequestList)
	for _, req := range recentRequests {
		keepList.AddRequest(req)
	}

	currentList := vm.requestHosts.Get(hostURI)
	if currentList != nil {
		for _, req := range *currentList {
			if !keepList.Contains(req) {
				vm.requestURIs.RemoveEntry(req.URI, req)
				continue
			}
			keepList.AddRequest(req)
		}
	}
	vm.requestHosts.SetList(hostURI, keepList)
}

func (vm *viewManager) AddArchiveRequest(hostURI, archiveURI string, request *ArchiveRequest) {
	vm.requestHosts.Set(hostURI, request)
	vm.requestURIs.Set(archiveURI, request)
}

func (vm *viewManager) RemoveArchiveRequests(hostURI string) {
	requests := vm.requestHosts.Get(hostURI)
	if requests == nil {
		return
	}
	for _, request := range *requests {
		//request.cancel <- true
		vm.requestHosts.Del(hostURI)
		vm.requestURIs.RemoveEntry(request.URI, request)
	}
}

func (vm *viewManager) StopArchiving(archiveURI string) {
}
