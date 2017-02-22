// basic API for dealing with archive requests
package requests

import (
	"fmt"
	"strings"

	messages "github.com/gtfierro/pundat/archiver"
	bw2 "github.com/immesys/bw2bind"
	"github.com/pkg/errors"
)

type ArchiveRequest messages.ArchiveRequest

// Returns true if the two ArchiveRequests are equal
func (req *ArchiveRequest) SameAs(other *ArchiveRequest) bool {
	return (req != nil && other != nil) &&
		(req.URI == other.URI) &&
		(req.PO == other.PO) &&
		(req.UUIDExpr == other.UUIDExpr) &&
		(req.ValueExpr == other.ValueExpr) &&
		(req.TimeExpr == other.TimeExpr) &&
		(req.TimeParse == other.TimeParse) &&
		(req.Name == other.Name) &&
		(req.InheritMetadata == other.InheritMetadata)
}

// checks URI and Name
func (req *ArchiveRequest) LooseSameAs(other *ArchiveRequest) bool {
	return req != nil && other != nil && req.Name == other.Name && req.URI == other.URI
}

func (req *ArchiveRequest) Dump() {
	r := messages.ArchiveRequest(*req)
	r.Dump()
}

// pack the object for publishing
func (req *ArchiveRequest) GetPO() (bw2.PayloadObject, error) {
	return bw2.CreateMsgPackPayloadObject(bw2.FromDotForm("2.0.8.0"), req)
}

// Returns the set of ArchiveRequest objects at all points in the URI. This method
// attaches the `/!meta/giles` suffix to the provided URI pattern, so this also works
// with `+` and `*` in the URI if your permissions allow.
func GetArchiveRequests(client *bw2.BW2Client, uri string) ([]*ArchiveRequest, error) {
	// generate the query URI
	uri = strings.TrimSuffix(uri, "/") + "/!meta/giles"
	fmt.Printf("RETRIEVING from %s\n", uri)
	queryResults, err := client.Query(&bw2.QueryParams{
		URI:       uri,
		AutoChain: true,
	})
	var requests []*ArchiveRequest
	if err != nil {
		return requests, err
	}
	for msg := range queryResults {
		for _, po := range msg.POs {
			if po.IsTypeDF(bw2.PODFGilesArchiveRequest) {
				var req = new(ArchiveRequest)
				if err := po.(bw2.MsgPackPayloadObject).ValueInto(&req); err != nil {
					return requests, err
				}
				req.FromVK = msg.From
				requests = append(requests, req)
			}
		}
	}
	return requests, nil
}

func RemoveAllArchiveRequests(client *bw2.BW2Client, uri string) error {
	uriFull := strings.TrimSuffix(uri, "/") + "/!meta/giles"
	fmt.Printf("DELETING ALL on %s\n", uriFull)
	// delete all
	return client.Publish(&bw2.PublishParams{
		URI:            uriFull,
		PayloadObjects: []bw2.PayloadObject{},
		Persist:        true,
		AutoChain:      true,
	})
	return nil
}

func RemoveArchiveRequestsFromConfig(client *bw2.BW2Client, configFile string, uri string) error {
	config, err := ReadConfig(configFile)
	if err != nil {
		return errors.Wrap(err, "Could not read config")
	}
	var remove []*ArchiveRequest
	for _, req := range config.DummyArchiveRequests {
		remove = append(remove, req.ToArchiveRequest())
	}
	return RemoveArchiveRequestList(client, uri, remove...)
}

// removes the set of archive requests using URI and Name
func RemoveArchiveRequestList(client *bw2.BW2Client, uri string, removeRequests ...*ArchiveRequest) error {
	uriFull := strings.TrimSuffix(uri, "/") + "/!meta/giles"
	requests, err := GetArchiveRequests(client, uriFull)
	if err != nil {
		return errors.Wrap(err, "Could not retrieve ArchiveRequests")
	}
	var keep []*ArchiveRequest
requestLoop:
	for _, req := range requests {
		for _, rem := range removeRequests {
			if rem.LooseSameAs(req) {
				continue requestLoop
			}
		}
		keep = append(keep, req)
	}
	err = AttachArchiveRequests(client, uri, keep...)
	if err != nil {
		return errors.Wrap(err, "Could not set ArchiveRequests")
	}

	return nil
}

func AddArchiveRequestsFromConfig(client *bw2.BW2Client, configFile, uri string) error {
	config, err := ReadConfig(configFile)
	if err != nil {
		return errors.Wrap(err, "Could not read config")
	}
	var attach []*ArchiveRequest
	for _, req := range config.DummyArchiveRequests {
		attach = append(attach, req.ToArchiveRequest())
	}
	return AttachArchiveRequests(client, uri, attach...)
}

// Attaches the archive request to the given URI. The request will be packed as a
// GilesArchiveRequestPID MsgPack object and attached to <uri>/!meta/giles.
// The URI does not have to be fully specified: if your permissions allow, you can
// also request that multiple URIs be archived using a `*` or `+` in the URI.
func AttachArchiveRequests(client *bw2.BW2Client, uri string, requests ...*ArchiveRequest) error {
	// sanity check the parameters
	if uri == "" {
		return errors.New("Need a valid URI")
	}
	for _, request := range requests {
		if request.PO == 0 {
			return errors.New("Need a valid PO number")
		}
		if request.ValueExpr == "" {
			return errors.New("Need a Value expression")
		}
		if request.Name == "" {
			return errors.New("Need a Name")
		}
	}

	// generate the publish URI
	uriFull := strings.TrimSuffix(uri, "/") + "/!meta/giles"

	existingRequests, err := GetArchiveRequests(client, uri)
	if err != nil {
		return errors.Wrap(err, "Could not fetch existing Archive Requests")
	}
	for _, existing := range existingRequests {
		for _, request := range requests {
			if request.LooseSameAs(existing) {
				return errors.New("Request already exists")
			}
		}
	}

	var pos []bw2.PayloadObject
	for _, req := range append(existingRequests, requests...) {
		if po, err := req.GetPO(); err == nil {
			pos = append(pos, po)
		} else {
			return err
		}
	}

	fmt.Printf("ATTACHING to %s\n", uriFull)
	// attach the metadata
	err = client.Publish(&bw2.PublishParams{
		URI:            uriFull,
		PayloadObjects: pos,
		Persist:        true,
		AutoChain:      true,
	})
	return err
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
