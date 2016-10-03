package dots

import (
	"github.com/immesys/bw2/objects"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"log"
)

type DotMaster struct {
	client *bw2.BW2Client
}

func NewDotMaster(client *bw2.BW2Client) *DotMaster {
	return &DotMaster{client}
}

func (dm *DotMaster) GetDOTChains(uri, vk string) ([]*objects.DChain, error) {
	var (
		chainlist []*objects.DChain
	)
	chains, err := dm.client.BuildChain(uri, "C", vk)
	if err != nil {
		return nil, err
	}
	for chain := range chains {
		obj, err := objects.NewDChain(objects.ROAccessDChain, chain.Content)
		dchain, ok := obj.(*objects.DChain)
		if !ok {
			// this shouldn't happen
			log.Fatal(errors.Wrap(err, "Could not get DOT from RoutingObject"))
		}
		num := dchain.NumHashes()
		for i := 0; i < num; i++ {
			dot := dchain.GetDOT(i)
			if dot == nil {
				hash := dchain.GetDotHash(i)
				ro, valid, err := dm.client.ResolveRegistry(objects.FmtHash(hash))
				if err != nil {
					log.Println(errors.Wrapf(err, "Could not resolve dot hash %s", hash))
					continue
				}
				if int(valid) != 1 {
					log.Println("Dot hash %v not valid", valid)
					continue
				}
				// the *objects.DOT we get is from the vendored package, so we need to
				// convert it to the objects package we can actually use
				_dot, err := objects.NewDOT(ro.GetRONum(), ro.GetContent())
				if err != nil {
					log.Println(errors.Wrap(err, "Could not convert DOT"))
					continue
				}
				dot = _dot.(*objects.DOT)
				dchain.SetDOT(i, dot)
			}
		}
		chainlist = append(chainlist, dchain)
	}
	return chainlist, nil
}
