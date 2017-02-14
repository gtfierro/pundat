package client

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	messages "github.com/gtfierro/pundat/archiver"
	bw "gopkg.in/immesys/bw2bind.v5"
)

const GilesQueryChangedRangesPIDString = "2.0.8.8"

var GilesQueryChangedRangesPID = bw.FromDotForm(GilesQueryChangedRangesPIDString)

var ErrNoResponse = errors.New("No response from archiver")

func init() {
	rand.Seed(time.Now().UnixNano())
}

type PundatClient struct {
	client  *bw.BW2Client
	vk      string
	uri     string
	c       chan *bw.SimpleMessage
	waiting map[uint32]chan *bw.SimpleMessage
	l       sync.RWMutex
}

// Create a new API isntance w/ the given client and VerifyingKey.
// The verifying key is returned by any of the BW2Client.SetEntity* calls
// URI should be the base of the giles service
func NewPundatClient(client *bw.BW2Client, vk string, uri string) (*PundatClient, error) {
	pc := &PundatClient{
		client:  client,
		vk:      vk,
		uri:     strings.TrimSuffix(uri, "/") + "/s.giles/_/i.archiver",
		waiting: make(map[uint32]chan *bw.SimpleMessage),
	}
	c, err := pc.client.Subscribe(&bw.SubscribeParams{
		URI: pc.uri + fmt.Sprintf("/signal/%s,queries", pc.vk[:len(pc.vk)-1]),
	})
	if err != nil {
		return nil, fmt.Errorf("Could not subscribe (%v)", err)
	}
	pc.c = c

	go func() {
		// subscribe and decode messages to get nonces
		for msg := range pc.c {
			nonce, err := getNonce(msg)
			if err != nil {
				log.Println(fmt.Sprintf("Error fetching nonce (%v)", err))
				continue
			}
			pc.l.Lock()
			if replyChan, found := pc.waiting[nonce]; found {
				// throw it in if channel is listening, else drop it
				select {
				case replyChan <- msg:
					delete(pc.waiting, nonce)
				default:
				}
			}
			pc.l.Unlock()
		}
	}()

	return pc, nil
}

func (pc *PundatClient) markWaitFor(nonce uint32, replyChan chan *bw.SimpleMessage) {
	pc.l.Lock()
	pc.waiting[nonce] = replyChan
	pc.l.Unlock()
}

// Synchronously queries the archiver. If no reply message is received within the given timeout (in seconds), this method returns ErrNoResponse. If timeout is <= 0,
// this method will block until a response is received from the archiver; possibly forever!
func (pc *PundatClient) Query(query string, timeout int) (mdRes messages.QueryMetadataResult, tsRes messages.QueryTimeseriesResult, chRes messages.QueryChangedResult, err error) {
	var (
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

	var timeoutChan <-chan time.Time

	if timeout > 0 {
		timeoutChan = time.After(time.Duration(timeout) * time.Second)
	}

	replyChan := make(chan *bw.SimpleMessage, 1)
	pc.markWaitFor(nonce, replyChan)

	err = pc.client.Publish(&bw.PublishParams{
		URI:            pc.uri + "/slot/query",
		PayloadObjects: []bw.PayloadObject{msg.ToMsgPackBW()},
	})
	if err != nil {
		err = fmt.Errorf("Could not publish (%v)", err)
		return
	}
	for {
		select {
		case <-timeoutChan:
			err = ErrNoResponse
			return
		case msg := <-replyChan:
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
				if err != nil {
					return
				}
				return
			}
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

func getNonce(msg *bw.SimpleMessage) (uint32, error) {
	var (
		po                bw.PayloadObject
		changedResults    messages.QueryChangedResult
		timeseriesResults messages.QueryTimeseriesResult
		metadataResults   messages.QueryMetadataResult
		queryError        messages.QueryError
	)
	if po = msg.GetOnePODF(bw.PODFGilesQueryError); po != nil {
		err := po.(bw.MsgPackPayloadObject).ValueInto(&queryError)
		return queryError.Nonce, err
	}
	if po = msg.GetOnePODF(bw.PODFGilesMetadataResponse); po != nil {
		err := po.(bw.MsgPackPayloadObject).ValueInto(&metadataResults)
		return metadataResults.Nonce, err
	}
	if po = msg.GetOnePODF(bw.PODFGilesTimeseriesResponse); po != nil {
		err := po.(bw.MsgPackPayloadObject).ValueInto(&timeseriesResults)
		return timeseriesResults.Nonce, err
	}
	if po = msg.GetOnePODF(GilesQueryChangedRangesPIDString); po != nil {
		err := po.(bw.MsgPackPayloadObject).ValueInto(&changedResults)
		return changedResults.Nonce, err
	}
	return 0, fmt.Errorf("no nonce found?!")
}
