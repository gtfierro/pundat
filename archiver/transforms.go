package archiver

import (
	"github.com/gtfierro/durandal/common"
	bw2 "gopkg.in/immesys/bw2bind.v5"
)

func POsFromMetadataGroup(nonce uint32, groups []common.MetadataGroup) bw2.PayloadObject {
	mdRes := QueryMetadataResult{
		Nonce: nonce,
		Data:  []KeyValueMetadata{},
	}
	for _, group := range groups {
		group.RLock()
		md := KeyValueMetadata{
			UUID:     group.UUID.String(),
			Metadata: make(map[string]interface{}),
			Path:     "",
		}
		for _, record := range group.Records {
			md.Metadata[record.Key] = record.Value
		}
		mdRes.Data = append(mdRes.Data, md)
		group.RUnlock()
	}
	return mdRes.ToMsgPackBW()
}

func POsFromTimeseriesGroup(nonce uint32, groups []common.Timeseries) bw2.PayloadObject {
	tsRes := QueryTimeseriesResult{
		Nonce: nonce,
		Data:  []Timeseries{},
	}
	for _, group := range groups {
		ts := Timeseries{
			UUID:   group.UUID.String(),
			Path:   group.SrcURI,
			Times:  []uint64{},
			Values: []float64{},
		}
		for _, rdg := range group.Records {
			ts.Times = append(ts.Times, uint64(rdg.Time.UnixNano()))
			ts.Values = append(ts.Values, rdg.Value)
		}
		tsRes.Data = append(tsRes.Data, ts)
	}
	return tsRes.ToMsgPackBW()
}
