package archiver

import (
	"context"
	"math/rand"
	"sync"

	bw2 "github.com/immesys/bw2bind"
)

type subscriptionMux struct {
	c         chan *bw2.SimpleMessage
	receivers map[int]chan *bw2.SimpleMessage
	sync.RWMutex
}

func newSubscriptionMux(ctx context.Context, c chan *bw2.SimpleMessage) *subscriptionMux {
	mux := &subscriptionMux{
		c:         c,
		receivers: make(map[int]chan *bw2.SimpleMessage),
	}

	// if the context ends, we close up the receiving channels
	// When we receive a new message, we send (with blocking) on
	// all of the receiver channels; we want to have backpressure here
	go func() {
		for {
			select {
			case <-ctx.Done():
				mux.Lock()
				defer mux.Unlock()
				for _, r := range mux.receivers {
					close(r)
				}
				return
			case msg := <-mux.c:
				mux.RLock()
				for _, r := range mux.receivers {
					r <- msg
				}
				mux.RUnlock()
			}
		}
	}()

	return mux
}

func (mux *subscriptionMux) add(r chan *bw2.SimpleMessage) (handle int) {
	mux.Lock()
	defer mux.Unlock()

	// get a unique handle
	found := true
	for found {
		handle = rand.Intn(32768)
		_, found = mux.receivers[handle]
	}
	mux.receivers[handle] = r
	return
}

func (mux *subscriptionMux) remove(handle int) {
	mux.Lock()
	defer mux.Unlock()
	delete(mux.receivers, handle)
}
