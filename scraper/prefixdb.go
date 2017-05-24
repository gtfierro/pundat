package main

import (
	"bytes"
	"fmt"

	"github.com/gtfierro/pundat/common"
	"github.com/syndtr/goleveldb/leveldb"
	"gopkg.in/mgo.v2/bson"
	//ldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type Config struct {
	path string
}

/*
In order to do proper metadata inheritance, we need something like a prefix tree,
except it stores the key/value pairs in the leaves, so that we can accumulate them
as we walk down the tree because a query for 'metadata for this URI' is really just
finding the longest prefix match!


build off of leveldb? byte prefix scans!!/iterators
*/

type PrefixDB struct {
	*leveldb.DB
}

func NewPrefixDB(cfg *Config) *PrefixDB {
	db, err := leveldb.OpenFile(cfg.path, nil)
	if err != nil {
		log.Fatal(err)
	}
	pfxdb := &PrefixDB{
		DB: db,
	}

	return pfxdb
}

func (pfxdb *PrefixDB) InsertRecords(records ...common.MetadataRecord) error {
	tx, err := pfxdb.OpenTransaction()
	if err != nil {
		return err
	}

	for _, rec := range records {
		kv := newKVPair()
		kv.Key = rec.Key
		kv.Value = rec.Value.(string)
		bytes, err := kv.MarshalMsg(nil)
		if err != nil {
			tx.Discard()
			return err
		}
		if rec.Key != "lastalive" {
			fmt.Println("insert", rec.SrcURI, rec.Key, rec.Value.(string))
		}
		if err := tx.Put([]byte(rec.SrcURI), bytes, nil); err != nil {
			tx.Discard()
			return err
		}
		kv.release()
	}

	return tx.Commit()
}

func (pfxdb *PrefixDB) GetPrefix(prefix []byte) ([]*KVPair, error) {
	//TODO: resolve namespace to the VK
	// nvrnSE4pJe4ZMO3WQdb-EPi5iwuzmTVUpk6XNNRGYsc=/eecs/sdh/s.KETIMote/2505/i.keti-temperature
	var pairs []*KVPair

	iter := pfxdb.NewIterator(BytesPrefix(prefix), nil)
	for iter.Next() {
		kv := newKVPair()
		if _, err := kv.UnmarshalMsg(iter.Value()); err != nil {
			iter.Release()
			return pairs, err
		}
		pairs = append(pairs, kv)
	}
	iter.Release()
	return pairs, iter.Error()
}

// accumulate all prefixes of 'uri'
func (pfxdb *PrefixDB) Lookup(uri string) bson.M {
	doc := make(bson.M)
	uribytes := []byte(uri)
	start := 0
	sep := []byte{'/'}
	index := bytes.Index(uribytes, sep)
	for index > -1 {
		kvp, err := pfxdb.GetPrefix(uribytes[:start+index])
		if err != nil {
			for _, kv := range kvp {
				kv.release()
			}
			log.Fatal(err)
		}
		if kvp != nil {
			for _, kv := range kvp {
				doc[kv.Key] = kv.Value
			}
		}
		for _, kv := range kvp {
			kv.release()
		}
		start += index + 1 // add one for '/'
		index = bytes.Index(uribytes[start:], sep)
	}
	// do the last bit that doesn't have a trailing '/'
	kvp, err := pfxdb.GetPrefix(uribytes)
	if err != nil {
		for _, kv := range kvp {
			kv.release()
		}
		log.Fatal(err)
	}
	if kvp != nil {
		for _, kv := range kvp {
			doc[kv.Key] = kv.Value
		}
	}
	for _, kv := range kvp {
		kv.release()
	}

	return doc
}
