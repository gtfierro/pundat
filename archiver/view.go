package archiver

import (
	"encoding/base64"
	"github.com/gtfierro/durandal/common"
	ob "github.com/gtfierro/giles2/objectbuilder"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"reflect"
	"strings"
	"time"
)

// takes care of handling/parsing archive requests
type viewManager struct {
	client *bw2.BW2Client
	// map of alias -> VK namespace
	namespaceAliases map[string]string
	requestHosts     *SynchronizedArchiveRequestMap
	requestURIs      *SynchronizedArchiveRequestMap
	muxer            *SubscriberMultiplexer
}

func newViewManager(client *bw2.BW2Client) *viewManager {
	return &viewManager{
		client:           client,
		namespaceAliases: make(map[string]string),
		requestHosts:     NewSynchronizedArchiveRequestMap(),
		requestURIs:      NewSynchronizedArchiveRequestMap(),
		muxer:            NewSubscriberMultiplexer(client),
	}
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

	common.NewWorkerPool(sub, func(msg *bw2.SimpleMessage) {
		parts := strings.Split(msg.URI, "/")
		key := parts[len(parts)-1]
		if key != "giles" {
			return
		}
		var requests []*ArchiveRequest
		// find list of existing requests at the received URI. Given the list of ones
		// that are there NOW, we remove the extras
		for _, po := range msg.POs {
			if !po.IsTypeDF(bw2.PODFGilesArchiveRequest) {
				continue
			}
			var request = new(ArchiveRequest)
			request.cancel = make(chan bool)
			err := po.(bw2.MsgPackPayloadObject).ValueInto(request)
			if err != nil {
				log.Error(errors.Wrap(err, "Could not parse Archive Request"))
				continue
			}
			if request.PO == 0 {
				log.Error(errors.Wrap(err, "Request contained no PO"))
				continue
			}
			if request.Value == "" {
				log.Error(errors.Wrap(err, "Request contained no Value expression"))
				continue
			}
			request.FromVK = msg.From
			if request.URI == "" { // no URI supplied
				request.URI = strings.TrimSuffix(request.URI, "!meta/giles")
				request.URI = strings.TrimSuffix(request.URI, "/")
			}
			if len(request.MetadataURIs) == 0 {
				request.MetadataURIs = []string{request.URI}
			}
			// TODO: does the FROM VK have permission to ask this?
			requests = append(requests, request)
		}
		// TODO: handle requests
		for _, request := range requests {
			if err := vm.HandleArchiveRequest(request); err != nil {
				log.Error(errors.Wrapf(err, "Could not handle archive request %+v", request))
			}
		}
	}, 1000).Start()

	query, err := vm.client.Query(&bw2.QueryParams{
		URI: namespace,
	})
	if err != nil {
		log.Error(errors.Wrap(err, "Could not subscribe"))
	}
	for msg := range query {
		sub <- msg
	}
}

func (vm *viewManager) HandleArchiveRequest(request *ArchiveRequest) error {
	//TODO: need a mapping from the archive
	// requests to the URI that provided them so that we
	// can detect when an archive request is removed
	if request.FromVK == "" {
		return errors.New("VK was empty in ArchiveRequest")
	}
	request.value = ob.Parse(request.Value)

	if request.UUID == "" {
		request.UUID = uuid.NewV3(NAMESPACE_UUID, request.URI+string(request.PO)+request.Value).String()
	} else {
		request.uuid = ob.Parse(request.UUID)
	}

	if request.Time != "" {
		request.time = ob.Parse(request.Time)
	}

	if request.MetadataExpr != "" {
		request.metadataExpr = ob.Parse(request.MetadataExpr)
	}
	var ret = common.NewMetadataGroup()
	if request.InheritMetadata {
		md, from, err := vm.client.GetMetadata(request.URI)
		if err != nil {
			return err
		}
		if len(request.UUID) == 0 && len(request.uuidActual) == 0 {
			request.uuidActual = common.UUID(request.UUID)
		}
		for k, v := range md {
			rec := &common.MetadataRecord{
				Key:   k,
				Value: v.Value,
				//TODO: check if metadatatuple timestamps are
				// nanoseconds or seconds
				TimeValid: time.Unix(0, v.Timestamp),
			}
			if srcURI, found := from[k]; found {
				rec.SrcURI = srcURI
			} else {
				rec.SrcURI = request.URI
			}
			ret.AddRecord(rec)
		}
		ret.UUID = request.uuidActual
		//TODO: save metadata or queue it to be saved
	}
	if len(request.MetadataURIs) > 0 {
		// need to query/subscribe for each of these
		// need to be able to cancel the subscription and
		// reclaim the goroutines if we stop archiving this
		// TODO: do this only once per archive URI
		for _, metadataURI := range request.MetadataURIs {
			uri := strings.TrimSuffix(metadataURI, "/") + "/!meta/+"
			mdSub, err := vm.muxer.AddSubscription(uri)
			if err != nil {
				return errors.Wrap(err, "Could not subscribe")
			}
			mdQuery, err := vm.client.Query(&bw2.QueryParams{
				URI: uri,
			})
			if err != nil {
				return errors.Wrap(err, "Could not subscribe")
			}
			go func(a chan *bw2.SimpleMessage) {
				for {
					select {
					case msg := <-a:
						msg.Dump()
					case <-request.cancel:
						log.Warning("Canceling request")
						close(a)
					}
				}
			}(mdSub)
			for msg := range mdQuery {
				mdSub <- msg
			}
		}
	}
	// by now, we've accumulated the initial set of metadata available to be associated
	// with this stream, so we save it to the metadata database

	// we now subscribe to the actual URI indicated by the archiverequest
	// we want to make sure that we don't subscribe more than necessary:
	// need a data structure that maps a URI (ending in "!meta/giles") to a set of
	// archive requests published on that URI AND a data structure that maps
	// the archive URI to the list of its archive requests

	/*
	   What's the API we need?
	   AddArchiveRequest(hostURI, archiveURI string, req *ArchiveRequest)
	   RemoveArchiveRequests(hostURI)
	   StopArchiving(archiveURI)
	*/

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
		request.cancel <- true
		vm.requestHosts.Del(hostURI)
		vm.requestURIs.RemoveEntry(request.URI, request)
	}
}

func (vm *viewManager) StopArchiving(archiveURI string) {
}
