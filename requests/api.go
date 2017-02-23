// basic API for dealing with archive requests
package requests

import (
	"fmt"
	"strings"

	messages "github.com/gtfierro/pundat/archiver"
	"github.com/immesys/bw2/objects"
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
	return req != nil && other != nil && req.Name == other.Name && req.URI == other.URI && req.AttachURI == other.AttachURI
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
	uri = addGilesSuffix(uri)
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
				req.AttachURI = normalizeNamespace(client, msg.URI)
				requests = append(requests, req)
			}
		}
	}
	return requests, nil
}

func RemoveAllArchiveRequests(client *bw2.BW2Client, uri string) error {
	uriFull := addGilesSuffix(uri)
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

func RemoveArchiveRequestsFromConfig(client *bw2.BW2Client, configFile string) error {
	config, err := ReadConfig(configFile)
	if err != nil {
		return errors.Wrap(err, "Could not read config")
	}
	var remove []*ArchiveRequest
	for _, req := range config.DummyArchiveRequests {
		remove = append(remove, req.ToArchiveRequest())
	}
	return RemoveArchiveRequestList(client, remove...)
}

// removes the set of archive requests using URI and Name
func RemoveArchiveRequestList(client *bw2.BW2Client, removeRequests ...*ArchiveRequest) error {
	var scanuris = make(map[string]struct{})
	// make list of URIs to check
	for _, rem := range removeRequests {
		rem.AttachURI = normalizeNamespace(client, rem.AttachURI)
		scanuris[rem.AttachURI] = struct{}{}
		rem.AttachURI = addGilesSuffix(rem.AttachURI)
	}

	// for each uri, get the list of archive requests. We filter this by those that we want
	// removed, and then re-publish the altered list
	for uri := range scanuris {
		fmt.Println(uri)
		var keep []*ArchiveRequest
		requests, err := GetArchiveRequests(client, uri)
		if err != nil {
			return err
		}
	requestLoop:
		for _, req := range requests {
			for _, rem := range removeRequests {
				if rem.LooseSameAs(req) {
					continue requestLoop
				}
			}
			keep = append(keep, req)
		}
		// re-attach what's left
		if len(keep) == 0 {
			return RemoveAllArchiveRequests(client, uri)
		} else {
			err = AttachArchiveRequests(client, keep...)
			if err != nil {
				return errors.Wrap(err, "Could not set ArchiveRequests")
			}
		}
	}

	return nil
}

func AddArchiveRequestsFromConfig(client *bw2.BW2Client, configFile string) error {
	config, err := ReadConfig(configFile)
	if err != nil {
		return errors.Wrap(err, "Could not read config")
	}
	var attach []*ArchiveRequest
	for _, req := range config.DummyArchiveRequests {
		attach = append(attach, req.ToArchiveRequest())
	}
	return MergeArchiveRequests(client, attach...)
}

func AttachArchiveRequests(client *bw2.BW2Client, requests ...*ArchiveRequest) error {
	var toadd = make(map[string][]*ArchiveRequest)
	// sanity check the parameters
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
		request.AttachURI = normalizeNamespace(client, addGilesSuffix(request.AttachURI))
		toadd[request.AttachURI] = append(toadd[request.AttachURI], request)
	}
	for uri, requests := range toadd {
		var pos []bw2.PayloadObject
		for _, req := range requests {
			if po, err := req.GetPO(); err == nil {
				pos = append(pos, po)
			} else {
				return err
			}
		}

		fmt.Printf("ATTACHING to %s\n", uri)
		// attach the metadata
		err := client.Publish(&bw2.PublishParams{
			URI:            uri,
			PayloadObjects: pos,
			Persist:        true,
			AutoChain:      true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Attaches the archive request to the given URI. The request will be packed as a
// GilesArchiveRequestPID MsgPack object and attached to <uri>/!meta/giles.
// The URI does not have to be fully specified: if your permissions allow, you can
// also request that multiple URIs be archived using a `*` or `+` in the URI.
func MergeArchiveRequests(client *bw2.BW2Client, requests ...*ArchiveRequest) error {
	// sanity check the parameters
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
		request.AttachURI = normalizeNamespace(client, addGilesSuffix(request.AttachURI))
	}

requestLoop:
	for _, request := range requests {
		existingRequests, err := GetArchiveRequests(client, request.AttachURI)
		if err != nil {
			return errors.Wrap(err, "Could not fetch existing Archive Requests")
		}
		// add requests to existingRequests if they are not already in there
		for _, existing := range existingRequests {
			if request.LooseSameAs(existing) {
				fmt.Println("Request already exists:")
				existing.Dump()
				continue requestLoop
			}
		}
		existingRequests = append(existingRequests, request)
		var pos []bw2.PayloadObject
		for _, req := range existingRequests {
			if po, err := req.GetPO(); err == nil {
				pos = append(pos, po)
			} else {
				return err
			}
		}

		fmt.Printf("ATTACHING to %s\n", request.AttachURI)
		// attach the metadata
		err = client.Publish(&bw2.PublishParams{
			URI:            request.AttachURI,
			PayloadObjects: pos,
			Persist:        true,
			AutoChain:      true,
		})
		if err != nil {
			return err
		}
	}
	return nil
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

// if the string does not end with /!meta/giles, it adds it
func addGilesSuffix(uri string) string {
	if !strings.HasSuffix(uri, "/!meta/giles") {
		return strings.TrimSuffix(uri, "/") + "/!meta/giles"
	}
	return uri
}

func resolveAliasToVK(client *bw2.BW2Client, alias string) string {
	obj, validity, err := client.ResolveRegistry(alias)
	if err != nil {
		fmt.Println("Error resolving alias", err)
		return alias
	}
	if validity != bw2.StateValid {
		fmt.Printf("Alias %s is not valid\n", alias)
		return alias
	}

	ent, ok := obj.(*objects.Entity)
	if !ok {
		fmt.Printf("Alias was not an entity: %s\n", alias)
		return alias
	}
	return objects.FmtKey(ent.GetVK())
}

// replaces the namespace alias in the URI with its VK
func normalizeNamespace(client *bw2.BW2Client, uri string) string {
	components := strings.SplitN(uri, "/", 2)
	return resolveAliasToVK(client, components[0]) + "/" + components[1]
}
