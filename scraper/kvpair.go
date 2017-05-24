//go:generate msgp
package main

import (
	"sync"
)

var _KVPAIRPOOL = sync.Pool{
	New: func() interface{} {
		return &KVPair{}
	},
}

func newKVPair() *KVPair {
	return _KVPAIRPOOL.Get().(*KVPair)
}

type KVPair struct {
	Key   string
	Value string
}

func (kv *KVPair) release() {
	*kv = KVPair{}
	_KVPAIRPOOL.Put(kv)
}
