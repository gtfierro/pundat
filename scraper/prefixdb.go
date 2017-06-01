package scraper

import (
	"bytes"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/gtfierro/pundat/common"
	"github.com/syndtr/goleveldb/leveldb"
	ldbutil "github.com/syndtr/goleveldb/leveldb/util"
	"github.com/willf/bloom"
	"gopkg.in/mgo.v2/bson"
)

type Config struct {
	path string
}

type URI string

func (a URI) Less(b btree.Item) bool {
	return a < b.(URI)
}

/*
In order to do proper metadata inheritance, we need something like a prefix tree,
except it stores the key/value pairs in the leaves, so that we can accumulate them
as we walk down the tree because a query for 'metadata for this URI' is really just
finding the longest prefix match!


build off of leveldb? byte prefix scans!!/iterators
*/

type PrefixDB struct {
	dependencies *leveldb.DB
	usagefilter  *bloom.BloomFilter
	updated      *btree.BTree
	mu           sync.Mutex
	*leveldb.DB
}

func NewPrefixDB(cfg *Config) *PrefixDB {
	db, err := leveldb.OpenFile(filepath.Join(cfg.path, "pfx"), nil)
	if err != nil {
		log.Fatal(err)
	}
	pfxdb := &PrefixDB{
		DB:          db,
		usagefilter: bloom.NewWithEstimates(1e6, .01),
		updated:     btree.New(3),
	}
	// dependency db
	db2, err := leveldb.OpenFile(filepath.Join(cfg.path, "dep"), nil)
	if err != nil {
		log.Fatal(err)
	}
	pfxdb.dependencies = db2

	// background compaction
	go func() {
		for _ = range time.Tick(15 * time.Minute) {
			log.Notice("Compacting Prefix DB")
			if err := db.CompactRange(ldbutil.Range{nil, nil}); err != nil {
				log.Error(err)
			}
			if err := db2.CompactRange(ldbutil.Range{nil, nil}); err != nil {
				log.Error(err)
			}
		}
	}()

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
		if err := tx.Put([]byte(rec.SrcURI), bytes, nil); err != nil {
			tx.Discard()
			return err
		}
		if pfxdb.usagefilter.Test([]byte(getStrippedURI(rec.SrcURI))) {
			// if the updated URI has been used, we need to mark it as 'dirty'
			// so the downstream documents can be updated
			stripped := getStrippedURI(rec.SrcURI)
			pfxdb.mu.Lock()
			for _, dep := range pfxdb.GetDependencies(stripped) {
				pfxdb.updated.ReplaceOrInsert(URI(dep))
			}
			pfxdb.mu.Unlock()
		}
		kv.release()
	}

	return tx.Commit()
}

// fetches all of the documents whose URIs are in the 'updated' tree
func (pfxdb *PrefixDB) GetUpdatedDocuments() []bson.M {
	var records []bson.M
	pfxdb.mu.Lock()
	touse := pfxdb.updated.Clone()
	pfxdb.updated = btree.New(3)
	pfxdb.mu.Unlock()

	max := touse.Max()
	iter := func(i btree.Item) bool {
		doc := pfxdb.Lookup(string(i.(URI)))
		doc["originaluri"] = string(i.(URI))
		records = append(records, doc)
		return i != max
	}
	touse.Ascend(iter)
	return records
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
		pfxdb.usagefilter.Add(uribytes[:start+index])
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
	pfxdb.usagefilter.Add(uribytes)
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

	// add this URI to the dependencies
	go func() {
		if tx, err := pfxdb.dependencies.OpenTransaction(); err != nil {
			log.Fatal(err)
		} else if err := tx.Put(uribytes, []byte{}, nil); err != nil {
			log.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}
	}()

	return doc
}

// given a URI prefix (stripped), returns all URIs it is a prefix of
// that someone has run 'Lookup' for
func (pfxdb *PrefixDB) GetDependencies(pfx string) []string {
	var dependencies []string
	db, err := pfxdb.dependencies.GetSnapshot()
	if err != nil {
		log.Fatal(err)
	}
	iter := db.NewIterator(ldbutil.BytesPrefix([]byte(pfx)), nil)
	for iter.Next() {
		dependencies = append(dependencies, string(iter.Key()))
	}
	iter.Release()
	db.Release()
	return dependencies
}
