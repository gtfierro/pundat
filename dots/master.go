package dots

import (
	"bytes"
	"encoding/base64"
	"github.com/immesys/bw2/objects"
	"github.com/immesys/bw2/util"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"log"
	"strings"
)

// the VK for the "archive" namespace
const ArchiveVK = "LMBbz7H1DeJICLTrEZk5EEpFGGmrZu-KiCllttWyZsI="

var ArchiveVKBytes, _ = base64.URLEncoding.DecodeString("LMBbz7H1DeJICLTrEZk5EEpFGGmrZu-KiCllttWyZsI=")

func fmtHash(hash []byte) string {
	return base64.URLEncoding.EncodeToString(hash)
}

func unFmtHash(s string) []byte {
	b, _ := base64.URLEncoding.DecodeString(s)
	return b
}

type DotMaster struct {
	client *bw2.BW2Client
}

func NewDotMaster(client *bw2.BW2Client) *DotMaster {
	return &DotMaster{client}
}

func (dm *DotMaster) GetValidRanges(uri, vk string) (*DisjointRanges, error) {
	var (
		ranges = new(DisjointRanges)
	)
	accessChains, err := dm.GetAccessDOTChains(uri, vk)
	if err != nil {
		return ranges, err
	}
	for _, dchain := range accessChains {
		rng := intersectDChainAccessTimes(dchain)
		ranges.Merge(rng)
	}
	archivalChains, err := dm.GetArchivalDOTChains(uri, vk)
	if err != nil {
		return ranges, err
	}
	for _, dchain := range archivalChains {
		rng := intersectDChainArchivalTimes(dchain)
		ranges.Merge(rng)
	}
	// take the union of all the found ranges
	return ranges, nil
}

func (dm *DotMaster) GetAccessDOTChains(uri, vk string) ([]*objects.DChain, error) {
	var (
		dot       *objects.DOT
		valid     bw2.RegistryValidity
		err       error
		chainlist []*objects.DChain
	)
	chains, err := dm.buildChain(uri, "C", vk)
	if err != nil {
		return nil, err
	}
	for _, chain := range chains {
		num := chain.NumHashes()
		for i := 0; i < num; i++ {
			dot = chain.GetDOT(i)
			// TODO: check that dot is valid
			if dot == nil {
				hash := chain.GetDotHash(i)
				dot, valid, err = dm.resolveRegistry(fmtHash(hash))
				if err != nil {
					log.Println(errors.Wrapf(err, "Could not resolve dot hash %s", hash))
					continue
				}
				if valid != bw2.StateValid {
					log.Println("Dot hash %v not valid", dm.client.ValidityToString(valid, err))
					continue
				}
				chain.SetDOT(i, dot)
			}
		}
		chainlist = append(chainlist, chain)
	}
	return chainlist, nil
}

// OKAY this actually has to be done differently
// First, we run client.FindDOTsFromVK with the "vk" as the namespace authority of the
// requested URI. We want to filter this result set by DOTs that are "on" the archive
// namespace, and then use that set to do our filtering
func (dm *DotMaster) GetArchivalDOTChains(uri, vk string) ([]*objects.DChain, error) {
	var nsvk string
	idx := strings.Index(uri, "/")
	if idx > 0 {
		nsvk = uri[:idx]
	} else {
		nsvk = uri
	}
	chains, err := dm.findDOTChains(nsvk, vk, uri)

	var dchainlist []*objects.DChain
	for _, chain := range chains {
		dchain, err := objects.CreateDChain(true, chain...)
		if err != nil {
			return dchainlist, err
		}
		dchainlist = append(dchainlist, dchain)
	}

	return dchainlist, err
}

// Searches for DOTs from VK [fromvk] to VK [findvk] on the archive version of the URI [uri]
// (which will be archive/start/+/end/+/<uri>)
// This method recurseively searches the graph and returns a list of the found DOTs
func (dm *DotMaster) findDOTChains(fromvk, findvk, uri string) ([][]*objects.DOT, error) {
	var (
		chains [][]*objects.DOT
	)
	dots, valids, err := dm.findDOTsFromVK(fromvk)
	if err != nil {
		return chains, errors.Wrap(err, "Could not find DOTS from vk")
	}
	for i, dot := range dots {
		var dots []*objects.DOT
		if valids[i] != bw2.StateValid {
			// skipping invalid DOT
			continue
		}
		if !bytes.Equal(dot.GetAccessURIMVK(), ArchiveVKBytes) {
			// skipping non-archive DOTs
			continue
		}
		// if the DOT is on the archive namespace and the archive URI pattern
		// covers the DOT, then we add the current DOT to our list of things to return
		archiveURI := getURIFromSuffix(dot.GetAccessURISuffix())
		_, matchesURI := util.RestrictBy(archiveURI, uri)
		if matchesURI {
			// if true, then this DOT is part of a chain
			dots = append(dots, dot)
			// if the receiver VK of this DOT is the one we are looking for,
			// then we end our search and continue onto the next found DOT
			recvVK := fmtHash(dot.GetReceiverVK())
			if recvVK == findvk {
				chains = append(chains, dots)
				continue
			}
			// else, we continue our search from the current matched URI and VK of this DOT
			recursive_chains, err := dm.findDOTChains(recvVK, findvk, uri)
			if err != nil {
				return chains, err
			}
			for _, chain := range recursive_chains {
				chains = append(chains, append(dots, chain...))
			}
		}
	}
	return chains, nil
}

// same as client.BuildChain, but converts to DChain that we can use
func (dm *DotMaster) buildChain(uri, perm, vk string) ([]*objects.DChain, error) {
	var chainlist []*objects.DChain
	chains, err := dm.client.BuildChain(uri, "C", vk)
	if err != nil {
		return chainlist, err
	}
	for chain := range chains {
		obj, err := objects.NewDChain(objects.ROAccessDChain, chain.Content)
		dchain, ok := obj.(*objects.DChain)
		if !ok {
			return chainlist, errors.Wrap(err, "Could not get DOT from RoutingObject")
		}
		chainlist = append(chainlist, dchain)
	}
	return chainlist, nil
}

func (dm *DotMaster) resolveRegistry(hash string) (*objects.DOT, bw2.RegistryValidity, error) {
	ro, valid, err := dm.client.ResolveRegistry(hash)
	if err != nil {
		return nil, bw2.StateUnknown, err
	}
	_dot, err := objects.NewDOT(ro.GetRONum(), ro.GetContent())
	if err != nil {
		return nil, bw2.StateUnknown, err
	}
	dot := _dot.(*objects.DOT)
	return dot, valid, nil
}

func (dm *DotMaster) findDOTsFromVK(fromvk string) ([]*objects.DOT, []bw2.RegistryValidity, error) {
	var (
		retDOTs []*objects.DOT
	)
	dots, valids, err := dm.client.FindDOTsFromVK(fromvk)
	if err != nil {
		return retDOTs, valids, err
	}
	for _, ro := range dots {
		_dot, err := objects.NewDOT(ro.GetRONum(), ro.GetContent())
		if err != nil {
			return retDOTs, valids, err
		}
		retDOTs = append(retDOTs, _dot.(*objects.DOT))
	}
	return retDOTs, valids, nil
}
