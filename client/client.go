package client

import (
	"fmt"
	messages "github.com/gtfierro/pundat/archiver"
	bw "gopkg.in/immesys/bw2bind.v5"
	"math/rand"
	"strings"
	"time"
)

const GilesQueryChangedRangesPIDString = "2.0.8.8"

var GilesQueryChangedRangesPID = bw.FromDotForm(GilesQueryChangedRangesPIDString)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type PundatClient struct {
	client *bw.BW2Client
	vk     string
	uri    string
}

// Create a new API isntance w/ the given client and VerifyingKey.
// The verifying key is returned by any of the BW2Client.SetEntity* calls
// URI should be the base of the giles service
func NewPundatClient(client *bw.BW2Client, vk string, uri string) *PundatClient {
	return &PundatClient{
		client: client,
		vk:     vk,
		uri:    strings.TrimSuffix(uri, "/") + "/s.giles/_/i.archiver",
	}
}

// synchronously queries
func (pc *PundatClient) Query(query string) (mdRes messages.QueryMetadataResult, tsRes messages.QueryTimeseriesResult, chRes messages.QueryChangedResult, err error) {
	var (
		c        chan *bw.SimpleMessage
		tag      string
		tsfound  bool
		mdfound  bool
		chfound  bool
		errfound bool
	)
	nonce := rand.Uint32()
	msg := messages.KeyValueQuery{
		Query: query,
		Nonce: nonce,
	}
	c, tag, err = pc.client.SubscribeH(&bw.SubscribeParams{
		URI: pc.uri + fmt.Sprintf("/signal/%s,queries", pc.vk[:len(pc.vk)-1]),
	})
	if err != nil {
		err = err
		return
	}
	err = pc.client.Publish(&bw.PublishParams{
		URI:            pc.uri + "/slot/query",
		PayloadObjects: []bw.PayloadObject{msg.ToMsgPackBW()},
	})
	if err != nil {
		return
	}
	fmt.Println("looking for", nonce)
	for msg := range c {
		errfound, err = getError(nonce, msg)
		if errfound {
			return
		}
		tsfound, tsRes, err = getTimeseries(nonce, msg)
		if err != nil {
			return
		}
		mdfound, mdRes, err = getMetadata(nonce, msg)
		if err != nil {
			return
		}
		chfound, chRes, err = getChanged(nonce, msg)
		if err != nil {
			return
		}
		if tsfound || mdfound || chfound {
			fmt.Println(tag)
			err = pc.client.Unsubscribe(tag)
			if err != nil {
				return
			}
			break
		}
	}
	return
}

// Extracts QueryError from Giles response. Returns false if no related message was found
func getError(nonce uint32, msg *bw.SimpleMessage) (bool, error) {
	var (
		po         bw.PayloadObject
		queryError messages.QueryError
	)
	if po = msg.GetOnePODF(bw.PODFGilesQueryError); po != nil {
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&queryError); err != nil {
			return false, err
		}
		if queryError.Nonce != nonce {
			return false, nil
		}
		return true, fmt.Errorf(queryError.Error)
	}
	return false, nil
}

// Extracts Metadata from Giles response. Returns false if no related message was found
func getMetadata(nonce uint32, msg *bw.SimpleMessage) (bool, messages.QueryMetadataResult, error) {
	var (
		po              bw.PayloadObject
		metadataResults messages.QueryMetadataResult
	)
	if po = msg.GetOnePODF(bw.PODFGilesMetadataResponse); po != nil {
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&metadataResults); err != nil {
			return false, metadataResults, err
		}
		if metadataResults.Nonce != nonce {
			return false, metadataResults, nil
		}
		return true, metadataResults, nil
	}
	return false, metadataResults, nil
}

// Extracts Timeseries from Giles response. Returns false if no related message was found
func getTimeseries(nonce uint32, msg *bw.SimpleMessage) (bool, messages.QueryTimeseriesResult, error) {
	var (
		po                bw.PayloadObject
		timeseriesResults messages.QueryTimeseriesResult
	)
	if po = msg.GetOnePODF(bw.PODFGilesTimeseriesResponse); po != nil {
		fmt.Println("found ts", nonce)
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&timeseriesResults); err != nil {
			return false, timeseriesResults, err
		}
		if timeseriesResults.Nonce != nonce {
			return false, timeseriesResults, nil
		}
		return true, timeseriesResults, nil
	}
	return false, timeseriesResults, nil
}

// Extracts Timeseries from Giles response. Returns false if no related message was found
func getChanged(nonce uint32, msg *bw.SimpleMessage) (bool, messages.QueryChangedResult, error) {
	var (
		po             bw.PayloadObject
		changedResults messages.QueryChangedResult
	)
	if po = msg.GetOnePODF(GilesQueryChangedRangesPIDString); po != nil {
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&changedResults); err != nil {
			return false, changedResults, err
		}
		if changedResults.Nonce != nonce {
			return false, changedResults, nil
		}
		return true, changedResults, nil
	}
	return false, changedResults, nil
}
