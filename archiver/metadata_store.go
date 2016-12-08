package archiver

import (
	"fmt"
	"github.com/gtfierro/pundat/common"
	"github.com/karlseguin/ccache"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"net"
	"time"
)

type groupedrecord struct {
	UUID    string `bson:"_id"`
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
	m.db = m.session.DB("pundat")
	m.metadata = m.db.C(c.collectionPrefix + "metadata")
	m.records = m.db.C(c.collectionPrefix + "records")
	m.mapping = m.db.C(c.collectionPrefix + "mapping")
	m.documents = m.db.C(c.collectionPrefix + "documents")

	// add indexes. This will fail Fatal
	m.addIndexes()

	go func() {
		for _ = range time.Tick(10 * time.Second) {
			var allUUIDs []string
			err := m.metadata.Find(bson.M{}).Distinct("uuid", &allUUIDs)
			if err != nil {
				log.Error(err)
				continue
			}
			startBlock := 0
			step := 1000
			endBlock := startBlock + step
			for startBlock < len(allUUIDs) {
				if endBlock > len(allUUIDs) {
					endBlock = len(allUUIDs)
				}
				log.Debug(startBlock, endBlock, len(allUUIDs))
				batch := allUUIDs[startBlock:endBlock]
				startBlock += step
				endBlock += step
				pipe := m.metadata.Pipe([]bson.M{{"$match": bson.M{"uuid": bson.M{"$in": batch}}}, {"$group": bson.M{"_id": "$uuid", "records": bson.M{"$push": "$$ROOT"}}}})
				// get iterator
				iter := pipe.Iter()
				var group groupedrecord
				var updates []interface{}
				for iter.Next(&group) {
					doc := bson.M{"uuid": group.UUID}
					for _, rec := range group.Records {
						if key, found := rec["key"]; !found {
							continue
						} else {
							doc[key.(string)] = rec["value"]
						}
					}
					uri, err := m.URIFromUUID(common.ParseUUID(group.UUID))
					if err != nil {
						log.Error(errors.Wrapf(err, "Could not fetch URI for uuid %s", group.UUID))
						continue
					}
					doc["path"] = uri
					updates = append(updates, bson.M{"uuid": group.UUID})
					updates = append(updates, bson.M{"$set": doc})
				}
				if err := iter.Close(); err != nil {
					log.Error(errors.Wrap(err, "Could not close iterator"))
					continue
				}
				log.Noticef("Updating metadata: %d updates", len(updates))
				bulk := m.documents.Bulk()
				bulk.Upsert(updates...)
				_, err = bulk.Run()
				if err != nil && !mgo.IsDup(err) {
					log.Error(errors.Wrap(err, "Could not do bulk operation"))
					continue
				} else if err == nil {
					//log.Infof("Bulk update: %d matched, %d modified", stats.Matched, stats.Modified)
				}

				// handle bulk error case from mongo
				if be, ok := err.(*mgo.BulkError); ok {
					if len(be.Cases()) == 0 {
						continue
					}
				}
				if err != nil {
					log.Error(err)
				}
			}
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

	index.Key = []string{"srcuri", "key"}
	err = m.records.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Could not create index on records.{srcuri,key} (%v)", err)
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
	//index.Key = []string{"$text:$**"}
	//index.Unique = false
	//index.DropDups = false
	//err = m.documents.EnsureIndex(index)
	//if err != nil {
	//	log.Fatalf("Could not create text index on documents (%v)", err)
	//}
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

func (m *mongoStore) SaveMetadata(records []*common.MetadataRecord) error {
	if len(records) == 0 {
		log.Infof("Aborting metadata insert with 0 records")
		return nil
	}

	// insert unmapped records
	inserts := make([]interface{}, len(records))
	for i, rec := range records {
		inserts[i] = rec
	}
	err := m.records.Insert(inserts...)
	if err != nil && !mgo.IsDup(err) {
		return errors.Wrap(err, "Could not insert")
	}

	var updatedUUIDs []common.UUID
	// attempt to update records for which a mapping exists
	for _, rec := range records {
		// need to "duplicate" each record by each of the streams it belongs to
		stripped := StripBangMeta(rec.SrcURI)
		uuids, err := m.pfx.GetUUIDsFromURI(stripped)
		if err != nil {
			return errors.Wrap(err, "Could not get UUIDs from URI")
		}
		updatedUUIDs = append(updatedUUIDs, uuids...)
		for _, u := range uuids {
			rec.UUID = u
			if _, err := m.metadata.Upsert(bson.M{"key": rec.Key, "srcuri": rec.SrcURI, "uuid": rec.UUID}, rec); err != nil && !mgo.IsDup(err) {
				return errors.Wrap(err, "Could not upsert")
			}
		}
	}

	// now we collect up documents so we can make queries on them
	// only update metadata if:
	// - it is in the list of UUIDs we have (use $match?
	// - there is actually metadata included
	pipe := m.metadata.Pipe([]bson.M{{"$match": bson.M{"uuid": bson.M{"$in": updatedUUIDs}}}, {"$group": bson.M{"_id": "$uuid", "records": bson.M{"$push": "$$ROOT"}}}})
	// get iterator
	iter := pipe.Iter()
	var group groupedrecord
	var updates []interface{}
	for iter.Next(&group) {
		doc := bson.M{"uuid": group.UUID}
		for _, rec := range group.Records {
			if key, found := rec["key"]; !found {
				continue
			} else {
				doc[key.(string)] = rec["value"]
			}
		}
		uri, err := m.URIFromUUID(common.ParseUUID(group.UUID))
		if err != nil {
			log.Error(errors.Wrapf(err, "Could not fetch URI for uuid %s", group.UUID))
			continue
		}
		doc["path"] = uri
		updates = append(updates, bson.M{"uuid": group.UUID})
		updates = append(updates, bson.M{"$set": doc})
	}
	if err := iter.Close(); err != nil {
		return errors.Wrap(err, "Could not close iterator")
	}
	bulk := m.documents.Bulk()
	bulk.Upsert(updates...)
	_, err = bulk.Run()
	if err != nil && !mgo.IsDup(err) {
		return errors.Wrap(err, "Could not do bulk operation")
	} else if err == nil {
		//log.Infof("Bulk update: %d matched, %d modified", stats.Matched, stats.Modified)
	}

	// handle bulk error case from mongo
	if be, ok := err.(*mgo.BulkError); ok {
		if len(be.Cases()) == 0 {
			return nil
		}
	}
	return err
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
	// associate the URI with this UUID
	if err := m.pfx.AddUUIDURIMapping(uri, uuid); err != nil {
		return errors.Wrap(err, "Could not save mapping of uri to uuid")
	}

	if err := m.mapping.Insert(bson.M{"uuid": uuid, "uri": uri}); err != nil && !mgo.IsDup(err) {
		return errors.Wrap(err, "Could not insert uuid,uri mapping")
	}

	// fetch URI for this UUID
	uri, err := m.URIFromUUID(uuid)
	if err != nil {
		log.Error(errors.Wrapf(err, "Could not fetch URI for uuid %s", uuid))
	}

	// make sure we deposit the UUID in the metadata table at the *very* least
	if err := m.metadata.Insert(bson.M{"uuid": uuid, "path": uri}); err != nil && !mgo.IsDup(err) {
		return errors.Wrap(err, "Could not insert UUID")
	}
	if _, err := m.documents.Upsert(bson.M{"uuid": uuid}, bson.M{"uuid": uuid, "path": uri}); err != nil && !mgo.IsDup(err) {
		return errors.Wrap(err, "Could not insert UUID")
	}

	// find existing metadata tags for each of the "prefixes" of the main URI
	mappedURIs, err := m.pfx.GetMetadataSuperstrings(uri)
	if err != nil {
		return err
	}
	// for these metadata URIs, copy their metadata into the MD database for this UUID
	var records []bson.M
	if err := m.records.Find(bson.M{"srcuri": bson.M{"$in": mappedURIs}}).All(&records); err != nil {
		return errors.Wrap(err, "Could not fetch records")
	}
	if len(records) > 0 {
		for _, rec := range records {
			rec["uuid"] = uuid
			upsert := bson.M{
				"key":    rec["key"],
				"srcuri": rec["srcuri"],
				"uuid":   rec["uuid"],
			}
			delete(rec, "_id")
			if _, err := m.metadata.Upsert(upsert, rec); err != nil && !mgo.IsDup(err) {
				log.Error(err)
				return err
			}
		}
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
