package archiver

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gtfierro/pundat/common"
	"github.com/karlseguin/ccache"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type groupedrecord struct {
	Prefix  string
	Records []bson.M
}

type mongoConfig struct {
	address          *net.TCPAddr
	collectionPrefix string
}

type mongoStore struct {
	session           *mgo.Session
	db                *mgo.Database
	metadata          *mgo.Collection
	documents         *mgo.Collection
	mapping           *mgo.Collection
	prefixRecords     *mgo.Collection
	prefixRecordsLock sync.Mutex

	uricache *ccache.Cache

	updatedPrefixes     map[string]struct{}
	updatedPrefixesLock sync.Mutex
}

func newMongoStore(c *mongoConfig) *mongoStore {
	var err error
	m := &mongoStore{
		uricache:        ccache.New(ccache.Configure()),
		updatedPrefixes: make(map[string]struct{}),
	}
	log.Noticef("Connecting to MongoDB at %v...", c.address.String())
	m.session, err = mgo.Dial(c.address.String())
	if err != nil {
		log.Criticalf("Could not connect to MongoDB: %v", err)
		return nil
	}
	log.Notice("...connected!")
	// fetch/create collections and db reference
	m.db = m.session.DB(c.collectionPrefix + "_pundat")
	m.metadata = m.db.C("metadata")
	m.mapping = m.db.C("mapping")
	m.documents = m.db.C("documents")
	m.prefixRecords = m.db.C("prefix_records")

	// add indexes. This will fail Fatal
	m.addIndexes()

	go func() {
		for _ = range time.Tick(10 * time.Second) {
			var updatedPrefixes []string
			m.updatedPrefixesLock.Lock()
			for pfx := range m.updatedPrefixes {
				updatedPrefixes = append(updatedPrefixes, pfx)
			}
			m.updatedPrefixes = make(map[string]struct{})
			m.updatedPrefixesLock.Unlock()
			t := time.Now()
			for _, pfx := range updatedPrefixes {
				// fetch the updates for this prefix
				var records bson.M
				err := m.prefixRecords.Find(bson.M{"__prefix": pfx}).One(&records)
				if err != nil {
					log.Error(errors.Wrap(err, "Problem fetching prefix updates"))
					continue
				}
				update := make(bson.M)
				for k, v := range records {
					if k == "__prefix" {
						continue // skip this
					}
					if record, ok := v.(bson.M); ok {
						update[k] = record["value"].(string)
					}
				}

				var uuidsToUpdate []string
				err = m.mapping.Find(bson.M{"uri": bson.M{"$regex": "^" + pfx + ""}}).Distinct("uuid", &uuidsToUpdate)
				if err != nil {
					log.Error(errors.Wrap(err, "Problem fetching matching uris for prefix"))
					continue
				}
				if len(uuidsToUpdate) == 0 {
					continue
				}
				chunksize := 100
				startBlock := 0
				endBlock := startBlock + chunksize
				for startBlock < len(uuidsToUpdate) {
					if endBlock > len(uuidsToUpdate) {
						endBlock = len(uuidsToUpdate)
					}
					batch := uuidsToUpdate[startBlock:endBlock]
					_, err := m.documents.UpdateAll(bson.M{"uuid": bson.M{"$in": batch}}, bson.M{"$set": update})
					if err != nil {
						log.Error(errors.Wrap(err, "Problem updating metadata for prefix"))
						continue
					}
					startBlock += chunksize
					endBlock += chunksize
				}
			}
			log.Noticef("Updated %d prefixes in %s", len(updatedPrefixes), time.Since(t))
		}
	}()

	return m
}

func (m *mongoStore) addIndexes() {
	var err error
	// create indexes
	index := mgo.Index{
		Key:        []string{"uuid", "srcuri", "key"},
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     false,
	}
	err = m.metadata.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Could not create index on metadata.{UUID, srcuri, key} (%v)", err)
	}

	index.Key = []string{"uri", "uuid"}
	err = m.mapping.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Could not create index on mapping.{uri,uuid} (%v)", err)
	}
	index.Key = []string{"path", "uuid"}
	err = m.documents.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Could not create index on documents.{uri,uuid} (%v)", err)
	}
	index.Key = []string{"__prefix"}
	err = m.prefixRecords.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Could not create index on prefix_records.{__prefix} (%v)", err)
	}
}

func (m *mongoStore) GetUnitOfTime(VK string, uuid common.UUID) (common.UnitOfTime, error) {
	var (
		c   int
		err error
		res interface{}
	)
	uot := common.UOT_S
	query := m.metadata.Find(bson.M{"uuid": uuid}).Select(bson.M{"UnitofTime": 1})
	if c, err = query.Count(); err != nil {
		return uot, errors.Wrapf(err, "Could not find any UnitofTime records")
	} else if c == 0 {
		return uot, fmt.Errorf("no stream named %v", uuid)
	}
	err = query.One(&res)
	if entry, found := res.(bson.M)["UnitofTime"]; found {
		if uotInt, isInt := entry.(int); isInt {
			uot = common.UnitOfTime(uotInt)
		} else {
			return uot, fmt.Errorf("Invalid UnitOfTime retrieved? %v", entry)
		}
		uot = common.UnitOfTime(entry.(int))
		if uot == 0 {
			uot = common.UOT_S
		}
	}
	return uot, nil
}

/*
Here we describe the mechanism for how to retrieve metadata using a given VK.
First, we run the unaltered query and retrieve the set of resulting docs. Then, we must
filter the results by:
- remove if the VK cannot build a chain to the URI of a returned stream
- if the VK cannot build a chain to the URI for a piece of metadata:
  - if the key is in tags, remove the result
  - if the key is in "where", remove the result

This requires testing and finish implementing the DOT stuff
*/
/*
This needs to work better;
first run GetUUIDs on the where clause, and then use that list of UUIDs as the new where clause
*/
func (m *mongoStore) GetMetadata(VK string, tags []string, where common.Dict) ([]common.MetadataGroup, error) {
	var (
		_results    []bson.M
		whereClause bson.M
		results     []common.MetadataGroup
	)

	selectTags := bson.M{"_id": 0}
	for _, tag := range tags {
		selectTags[tag] = 1
	}

	if len(where) != 0 {
		whereClause = where.ToBSON()
	}

	if err := m.documents.Find(whereClause).Select(selectTags).All(&_results); err != nil {
		return nil, errors.Wrap(err, "Could not select tags")
	}

	for _, doc := range _results {
		group := common.GroupFromBson(doc)
		if !group.IsEmpty() {
			results = append(results, *group)
		}
	}

	return results, nil
}

func (m *mongoStore) GetUUIDs(VK string, where common.Dict) ([]common.UUID, error) {
	var (
		whereClause bson.M
		_uuids      []string
	)
	if len(where) != 0 {
		whereClause = where.ToBSON()
	}
	staged := m.documents.Find(whereClause)
	if err := staged.Distinct("uuid", &_uuids); err != nil {
		return nil, errors.Wrap(err, "Could not select UUID")
	}
	var uuids []common.UUID
	for _, uuid := range _uuids {
		uuids = append(uuids, common.ParseUUID(uuid))
	}
	return uuids, nil
}

func (m *mongoStore) GetDistinct(VK string, tag string, where common.Dict) ([]string, error) {
	var (
		whereClause bson.M
		distincts   []string
	)
	if len(where) != 0 {
		whereClause = where.ToBSON()
	}
	if err := m.metadata.Find(whereClause).Distinct(tag, &distincts); err != nil {
		return nil, errors.Wrap(err, "Could not get the thing")
	}
	return distincts, nil
}

// save the records to prefixRecords, where they are grouped by their stripped prefix (which is their URI
// but without !meta/keyname at the end).
func (m *mongoStore) SaveMetadata(records []*common.MetadataRecord) error {
	if len(records) == 0 {
		log.Infof("Aborting metadata insert with 0 records")
		return nil
	}
	m.prefixRecordsLock.Lock()
	m.updatedPrefixesLock.Lock()
	defer m.updatedPrefixesLock.Unlock()
	defer m.prefixRecordsLock.Unlock()
	// now insert the updated metadata records, grouped by their stripped prefix
	for _, rec := range records {
		pfx := StripBangMeta(rec.SrcURI)
		m.updatedPrefixes[pfx] = struct{}{} // mark this prefix as needing updates
		update := bson.M{"$set": bson.M{rec.Key: rec}}
		_, err := m.prefixRecords.Upsert(bson.M{"__prefix": pfx}, update)
		if err != nil && !mgo.IsDup(err) {
			return errors.Wrapf(err, "Could not insert record %v into prefixRecords", rec)
		}
	}

	return nil
}

func (m *mongoStore) AddNameTag(name string, uuid common.UUID) error {
	updateFilter := bson.M{
		"uuid": uuid,
	}
	updateContents := bson.M{
		"$set": bson.M{"_name": name},
	}
	if err := m.documents.Update(updateFilter, updateContents); err != nil && !mgo.IsDup(err) {
		return err
	}
	return nil
}

func (m *mongoStore) RemoveMetadata(VK string, tags []string, where common.Dict) error {
	return nil
}

func (m *mongoStore) MapURItoUUID(uri string, uuid common.UUID) error {
	if m.uricache.Get(uri+uuid.String()) != nil {
		return nil
	}

	if err := m.mapping.Insert(bson.M{"uuid": uuid, "uri": uri}); err != nil && !mgo.IsDup(err) {
		return errors.Wrap(err, "Could not insert uuid,uri mapping")
	}
	if err := m.documents.Insert(bson.M{"uuid": uuid, "_uri": uri}); err != nil && !mgo.IsDup(err) {
		return errors.Wrap(err, "Could not insert uuid,uri new document")
	}

	m.uricache.Set(uri+uuid.String(), struct{}{}, 10*time.Minute)

	return nil
}

// need to get this from the actual archiverequests
func (m *mongoStore) URIFromUUID(uuid common.UUID) (uri string, err error) {
	var uris []string
	if err := m.mapping.Find(bson.M{"uuid": uuid}).Distinct("uri", &uris); err != nil {
		return "", errors.Wrapf(err, "Could not find URIs for UUID %s", uuid)
	}
	if len(uris) > 1 {
		return "", errors.Errorf("Got %d URIs for UUID %s, expected 1", len(uris), uuid)
	} else if len(uris) == 0 {
		return "", errors.Errorf("no URI found")
	}
	return uris[0], nil
}

func (m *mongoStore) URItoUUID(uri string) (common.UUID, error) {
	return nil, nil
}
