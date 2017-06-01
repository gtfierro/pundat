package archiver

import (
	"time"

	"github.com/gtfierro/pundat/common"
	"github.com/gtfierro/pundat/scraper"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type mongo_store2 struct {
	pfxdb     *scraper.PrefixDB
	session   *mgo.Session
	db        *mgo.Database
	documents *mgo.Collection
}

func newMongoStore2(c *mongoConfig) *mongo_store2 {
	var err error
	m := &mongo_store2{
		pfxdb: scraper.DB,
	}
	log.Noticef("Connecting to MongoDB at %v...", c.address.String())
	m.session, err = mgo.Dial(c.address.String())
	if err != nil {
		log.Criticalf("Could not connect to MongoDB: %v", err)
		return nil
	}
	log.Notice("...connected!")
	m.db = m.session.DB(c.collectionPrefix + "_pundat")
	m.documents = m.db.C("documents")
	// add indexes. This will fail Fatal
	m.addIndexes()

	go func() {
		for _ = range time.Tick(30 * time.Second) {
			var updates []interface{}
			for _, doc := range m.pfxdb.GetUpdatedDocuments() {
				delete(doc, "name")
				delete(doc, "unit")
				delete(doc, "uri")
				updates = append(updates, bson.M{"originaluri": doc["originaluri"]}, bson.M{"$set": doc})
			}
			batch := m.documents.Bulk()
			batch.UpdateAll(updates...)
			info, err := batch.Run()
			if err != nil {
				log.Error(err.(*mgo.BulkError))
				log.Error(errors.Wrap(err, "Could not update metadata"))
			}
			log.Info("Updated", info.Modified)
		}
	}()

	return m
}

func (m *mongo_store2) GetUnitOfTime(VK string, uuid common.UUID) (common.UnitOfTime, error) {
	return common.UOT_NS, nil
}

// TODO: implement
func (m *mongo_store2) GetMetadata(VK string, tags []string, where common.Dict) ([]common.MetadataGroup, error) {
	var (
		_results    []bson.M
		whereClause bson.M
		results     []common.MetadataGroup
	)

	// if we have tags, then we make sure to include path/uuid. Otherwise, we need to keep
	// selectTags using just exclusive (e.g. _id: 0) so that mongo knows to return all of the document
	selectTags := bson.M{"_id": 0}
	for _, tag := range tags {
		selectTags[tag] = 1
	}
	if len(tags) > 0 {
		selectTags["uri"] = 1
		selectTags["originaluri"] = 1
		selectTags["uuid"] = 1
	}

	if len(where) != 0 {
		whereClause = where.ToBSON()
	}

	if err := m.documents.Find(whereClause).Select(selectTags).All(&_results); err != nil {
		return nil, errors.Wrap(err, "Could not select tags")
	}

	log.Debug(_results)

	for _, doc := range _results {
		group := common.GroupFromBson(doc)
		if !group.IsEmpty() {
			results = append(results, *group)
		}
	}

	return results, nil
}

// TODO: implement
func (m *mongo_store2) GetDistinct(VK string, tag string, where common.Dict) ([]string, error) {
	var (
		whereClause bson.M
		distincts   []string
		v           []interface{}
	)
	if len(where) != 0 {
		whereClause = where.ToBSON()
	}
	if err := m.documents.Find(whereClause).Distinct(tag, &v); err != nil {
		return nil, errors.Wrap(err, "Could not get the thing")
	}
	for _, val := range v {
		if str, ok := val.(string); ok {
			distincts = append(distincts, str)
		}
	}
	return distincts, nil
}

// TODO: implement
func (m *mongo_store2) GetUUIDs(VK string, where common.Dict) ([]common.UUID, error) {
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

func (m *mongo_store2) AddNameTag(name string, uuid common.UUID) error {
	return nil
}
func (m *mongo_store2) RemoveMetadata(VK string, tags []string, where common.Dict) error {
	return nil
}
func (m *mongo_store2) URItoUUID(uri string) (common.UUID, error) {
	return common.UUID{}, nil
}
func (m *mongo_store2) URIFromUUID(uuid common.UUID) (string, error) {
	return "", nil
}

func (m *mongo_store2) InitializeURI(uri, rewrittenURI, name, unit string, uuid common.UUID) error {
	log.Info("initializing", uri, name, unit)
	doc := m.pfxdb.Lookup(uri)
	doc["name"] = name
	doc["unit"] = unit
	doc["originaluri"] = uri
	doc["uri"] = rewrittenURI
	doc["uuid"] = uuid.String()

	if _, insertErr := m.documents.Upsert(bson.M{"uuid": doc["uuid"]}, doc); insertErr != nil {
		return insertErr
	}

	return nil
}

func (m *mongo_store2) addIndexes() {
	index := mgo.Index{
		Key:        []string{"uuid"},
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     false,
	}
	err := m.documents.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Could not create index on metadata.{uuid} (%v)", err)
	}
	index.Key = []string{"uri", "name", "unit"}
	index.Unique = false
	index.DropDups = false
	err = m.documents.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Could not create index on metadata.{uuid} (%v)", err)
	}
}
