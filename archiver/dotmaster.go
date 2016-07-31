package archiver

import (
	"github.com/gtfierro/durandal/common"
	"github.com/immesys/bw2/objects"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"time"
)

// returns a copy of this group that only contains records for which:
//  - a valid DOT exists from the VK to the record
//  - the metadata record is valid w/n the valid time window of the DOT
type DotMaster struct {
	client *bw2.BW2Client
	//TODO: cache here
	//TODO: cache invalidation on blockchain (configurable block num)
}

func NewDotMaster(client *bw2.BW2Client, blockExpiry int) *DotMaster {
	return &DotMaster{
		client: client,
	}
}

// returns a copy of this group that only contains records for which:
//  - a valid DOT exists with C permission from the VK to the record
//  - TODO? the metadata record is valid w/n the valid time window of the DOT. do we need to check
//    this?
func (dm *DotMaster) FilterMDByVKRead(VK string, group *common.MetadataGroup) (*common.MetadataGroup, error) {
	//TODO: fetch from cache
	// first, build a chain from the VK to all URIs in the metadata group
	var ret = common.NewMetadataGroup()
	for _, rec := range group.Records {
		// check for valid chain
		chain, err := dm.client.BuildAnyChain(rec.SrcURI, "C", VK)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not build C chain to %s on %s", VK, rec.SrcURI)
		}

		if chain != nil {
			ret.AddRecord(rec)
		}
	}
	return ret, nil
}

// returns a copy of this group that only contains records for which:
//  - a valid DOT exists with P permission from the VK to the record
func (dm *DotMaster) FilterMDByVKWrite(VK string, group *common.MetadataGroup) (*common.MetadataGroup, error) {
	var ret = common.NewMetadataGroup()
	for _, rec := range group.Records {
		chain, err := dm.client.BuildAnyChain(rec.SrcURI, "P", VK)
		if err != nil {
			return nil, errors.Wrapf(err, "Could not build P chain to %s on %s", VK, rec.SrcURI)
		}
		if chain != nil {
			ret.AddRecord(rec)
		}
	}
	return ret, nil
}

// returns a copy of this set of timeseries that only contains records for which:
//  - a valid DOT exists with C permission from the VK to the URI
//  - the timeseries record is from the beginning/end of the VK's DOT to the URI
func (dm *DotMaster) FilterTSByVK(VK string, ts *common.Timeseries) (*common.Timeseries, error) {
	var ret = &common.Timeseries{}
	// find all chains to the timeseries URI
	chainChan, err := dm.client.BuildChain(ts.SrcURI, "C", VK)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not build C chain to %s on %s", VK, ts.SrcURI)
	}
	var (
		start = time.Now()
		end   = time.Now()
	)
	for chain := range chainChan {
		// get the DOT
		_dot, err := objects.NewDOT(objects.ROPermissionDOT, chain.Content)
		if err != nil {
			return nil, errors.Wrap(err, "Could not parse ROPermissionDOT from chain contents")
		}
		dot, ok := _dot.(*objects.DOT)
		if !ok {
			return nil, errors.Wrap(err, "Could not get DOT from RoutingObject")
		}
		dotstart := dot.GetCreated()
		dotend := dot.GetExpiry()
		if dotstart.Before(start) {
			start = *dotstart
		}
		if dotend.After(end) {
			end = *dotend
		}
	}
	// now filter timeseries by the start and end
	log.Debugf("Start: %v, End %v", start, end)
	for _, rec := range ts.Records {
		if rec.Time.After(start) && rec.Time.Before(end) {
			ret.AddRecord(rec)
		}
	}
	return ret, nil
}
