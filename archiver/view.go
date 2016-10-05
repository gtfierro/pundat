package archiver

import (
	"encoding/base64"
	"github.com/gtfierro/durandal/prefix"
	"github.com/gtfierro/ob"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"reflect"
	"strings"
)

// takes care of handling/parsing archive requests
type viewManager struct {
	client   *bw2.BW2Client
	store    MetadataStore
	ts       TimeseriesStore
	pfx      *prefix.PrefixStore
	subber   *metadatasubscriber
	incoming chan *bw2.SimpleMessage
	// map of alias -> VK namespace
	namespaceAliases map[string]string
	requestHosts     *SynchronizedArchiveRequestMap
	requestURIs      *SynchronizedArchiveRequestMap
	muxer            *SubscriberMultiplexer
}

func newViewManager(client *bw2.BW2Client, store MetadataStore, ts TimeseriesStore, pfx *prefix.PrefixStore, subber *metadatasubscriber) *viewManager {
	vm := &viewManager{
		client:           client,
		store:            store,
		ts:               ts,
		pfx:              pfx,
		subber:           subber,
		incoming:         make(chan *bw2.SimpleMessage, 100),
		namespaceAliases: make(map[string]string),
		requestHosts:     NewSynchronizedArchiveRequestMap(),
		requestURIs:      NewSynchronizedArchiveRequestMap(),
		muxer:            NewSubscriberMultiplexer(client),
	}
	go func() {
		for msg := range vm.incoming {
			parts := strings.Split(msg.URI, "/")
			key := parts[len(parts)-1]
			if key != "giles" {
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
				if request.PO == 0 {
					log.Error("Request contained no PO")
					continue
				}
				if len(request.Name) == 0 {
					log.Error("Request contained no Name")
				}
				if request.ValueExpr == "" {
					log.Error("Request contained no Value expression")
					continue
				}
				request.FromVK = msg.From
				if request.URI == "" { // no URI supplied
					request.URI = strings.TrimSuffix(request.URI, "!meta/giles")
					request.URI = strings.TrimSuffix(request.URI, "/")
				}
				chain, err := vm.client.BuildAnyChain(request.URI, "C", request.FromVK)
				if err != nil || chain == nil {
					log.Error(errors.Wrapf(err, "VK %s did not have permission to archive %s", request.FromVK, request.URI))
					continue
				}
				requests = append(requests, request)
			}
			for _, request := range requests {
				if err := vm.HandleArchiveRequest(request); err != nil {
					log.Error(errors.Wrapf(err, "Could not handle archive request %+v", request))
				}
			}
		}
	}()
	return vm
}

// Given a namespace, we subscribe to <ns>/*/!meta/giles. For each received message
// on the URI, we extract the list of ArchiveRequests
func (vm *viewManager) subscribeNamespace(ns string) {
	namespace := strings.TrimSuffix(ns, "/") + "/*/!meta/giles"

	ro, _, err := vm.client.ResolveRegistry(ns)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "Could not resolve namespace %s", ns))
	}
	// OKAY so the issue here is that bw2's objects package is vendored, and runs into
	// conflict when used with the bw2bind package. So, we cannot import the objects
	// package. We only need the objects package to get the *objects.Entity object from
	// the RoutingObject interface we get from calling ResolveRegistry. The reason why we
	// need an Entity object is so we can call its GetVK() method to get the namespace VK
	// that is mapped to by the alias we threw into ResolveRegistry.
	// Because the underlying object actually is an entity object, we can use the reflect
	// package to just call the method directly without having to import the objects
	// package to do the type conversion (e.g. ro.(*object.Entity).GetVK()).
	// The rest is just reflection crap: call the method using f.Call() using []reflect.Value
	// to indicate an empty arguments list. We use [0] to get the first (and only) result,
	// and call .Bytes() to return the underlying byte array returned by GetVK(). We
	// then interpret it using base64 urlsafe encoding to get the string value.
	f := reflect.ValueOf(ro).MethodByName("GetVK")
	nsvk := base64.URLEncoding.EncodeToString(f.Call([]reflect.Value{})[0].Bytes())
	vm.namespaceAliases[namespace] = nsvk
	log.Noticef("Resolved alias %s -> %s", namespace, nsvk)
	log.Noticef("Subscribe to %s", namespace)
	sub, err := vm.client.Subscribe(&bw2.SubscribeParams{
		URI: namespace,
	})
	if err != nil {
		log.Fatal(errors.Wrapf(err, "Could not subscribe to namespace %s", namespace))
	}

	go func() {
		for msg := range sub {
			vm.incoming <- msg
		}
	}()

	// handle archive requests that have already existed
	query, err := vm.client.Query(&bw2.QueryParams{
		URI: namespace,
	})
	if err != nil {
		log.Error(errors.Wrap(err, "Could not subscribe"))
	}
	go func() {
		for msg := range query {
			vm.incoming <- msg
		}
	}()
}

func (vm *viewManager) HandleArchiveRequest(request *ArchiveRequest) error {
	//TODO: need a mapping from the archive requests to the URI that provided them so that we can detect when an archive request is removed
	if request.FromVK == "" {
		return errors.New("VK was empty in ArchiveRequest")
	}

	// a Stream's URI is its subscription for timeseries data
	stream := &Stream{
		uri:             request.URI,
		name:            request.Name,
		cancel:          make(chan bool),
		valueString:     request.ValueExpr,
		inheritMetadata: request.InheritMetadata,
	}

	stream.valueExpr = ob.Parse(request.ValueExpr)

	if len(request.UUIDExpr) > 0 {
		stream.uuidExpr = ob.Parse(request.UUIDExpr)
	}

	if len(request.TimeExpr) > 0 {
		stream.timeExpr = ob.Parse(request.TimeExpr)
	}

	var metadataURIs []string
	if request.InheritMetadata {
		for _, uri := range GetURIPrefixes(request.URI) {
			metadataURIs = append(metadataURIs, uri+"/!meta/+")
		}
	}
	sub, err := vm.client.Subscribe(&bw2.SubscribeParams{
		URI: stream.uri,
	})
	if err != nil {
		return errors.Wrapf(err, "Could not subscribe to %s", stream.uri)
	}
	stream.subscription = sub

	for _, muri := range metadataURIs {
		vm.subber.requestSubscription(muri)
	}

	// indicate that we've gotten an archive request
	request.Dump()

	// now, we save the stream
	stream.startArchiving(vm.ts, vm.store)

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
