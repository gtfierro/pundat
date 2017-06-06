package archiver

import (
	"net"
	"time"

	"github.com/coocood/freecache"
	"github.com/gtfierro/pundat/common"
	"github.com/gtfierro/pundat/scraper"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type mongoConfig struct {
	address          *net.TCPAddr
	collectionPrefix string
}

type mongo_store struct {
	pfxdb     *scraper.PrefixDB
	session   *mgo.Session
	db        *mgo.Database
	documents *mgo.Collection
	uuidtouri *mgo.Collection
	doccache  *freecache.Cache
	uricache  *freecache.Cache
}

func newMongoStore(c *mongoConfig) *mongo_store {
	var err error
	m := &mongo_store{
		pfxdb:    scraper.DB,
		doccache: freecache.NewCache(50 * 1024 * 1024),
		uricache: freecache.NewCache(50 * 1024 * 1024),
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
	m.uuidtouri = m.db.C("uuidtouri")
	// add indexes. This will fail Fatal
	m.addIndexes()

	go func() {
		for _ = range time.Tick(30 * time.Second) {
			var updates []interface{}
			for _, doc := range m.pfxdb.GetUpdatedDocuments() {
				// get uuid from originaluri
				uuid, err := m.UUIDFromURI(doc["originaluri"].(string))
				if err != nil {
					log.Error(errors.Wrap(err, "Could not get UUID from URI"))
				}
				key := []byte(uuid.String())
				if bytes, err := bson.Marshal(doc); err != nil {
					log.Error(errors.Wrap(err, "Could not serialize bson.M doc for cache"))
				} else if err := m.doccache.Set(key, bytes, -1); err != nil {
					log.Error(errors.Wrap(err, "Could not add doc to cache"))
				}
				delete(doc, "name")
				delete(doc, "unit")
				delete(doc, "uri")
				updates = append(updates, bson.M{"originaluri": doc["originaluri"]}, bson.M{"$set": doc})
			}
			batch := m.documents.Bulk()
			batch.UpdateAll(updates...)
			if info, err := batch.Run(); err != nil && len(err.(*mgo.BulkError).Cases()) > 0 {
				log.Error(errors.Wrap(err.(*mgo.BulkError), "Could not update metadata"))
			} else if info != nil {
				log.Info("Updated", info.Modified)
			}
		}
	}()

	return m
}

func (m *mongo_store) GetUnitOfTime(VK string, uuid common.UUID) (common.UnitOfTime, error) {
	return common.UOT_NS, nil
}

func (m *mongo_store) GetMetadata(VK string, tags []string, where common.Dict) ([]common.MetadataGroup, error) {
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

	for _, doc := range _results {
		group := common.GroupFromBson(doc)
		if !group.IsEmpty() {
			results = append(results, *group)
		}
	}

	return results, nil
}

func (m *mongo_store) GetDistinct(VK string, tag string, where common.Dict) ([]string, error) {
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

func (m *mongo_store) GetUUIDs(VK string, where common.Dict) ([]common.UUID, error) {
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

func (m *mongo_store) URIFromUUID(uuid common.UUID) (string, error) {
	var uri interface{}
	err := m.uuidtouri.Find(bson.M{"uuid": uuid.String()}).Select(bson.M{"uri": 1}).One(&uri)
	return uri.(bson.M)["uri"].(string), err
}

func (m *mongo_store) UUIDFromURI(uri string) (common.UUID, error) {
	bytes, err := m.uricache.Get([]byte(uri))
	if err == freecache.ErrNotFound {
		var _uuid interface{}
		if err := m.uuidtouri.Find(bson.M{"uri": uri}).Select(bson.M{"uuid": 1}).One(&_uuid); err != nil {
			return nil, err
		}
		uuid := _uuid.(bson.M)["uuid"].(string)
		if err := m.uricache.Set([]byte(uri), []byte(uuid), -1); err != nil {
			return nil, err
		}
		return common.UUID(uuid), nil
	} else if err != nil {
		return nil, err
	}
	return common.UUID(string(bytes)), nil
}

func (m *mongo_store) InitializeURI(uri, rewrittenURI, name, unit string, uuid common.UUID) error {
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
	if _, insertErr := m.uuidtouri.Upsert(bson.M{"uuid": doc["uuid"]}, bson.M{"uuid": doc["uuid"], "uri": doc["uri"]}); insertErr != nil {
		return insertErr
	}
	if err := m.uricache.Set([]byte(uri), []byte(uuid.String()), -1); err != nil {
		return err
	}

	return nil
}

func (m *mongo_store) addIndexes() {
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

	index.Key = []string{"uri", "uuid"}
	index.Unique = true
	index.DropDups = true
	err = m.uuidtouri.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Could not create index on uuidtouri.{uri, uuid} (%v)", err)
	}
}

func (m *mongo_store) GetDocument(uuid common.UUID) bson.M {
	bytes, err := m.doccache.Get([]byte(uuid))
	if err == freecache.ErrNotFound {
		var mydoc bson.M
		if err := m.documents.Find(bson.M{"uuid": uuid.String()}).Select(bson.M{"_id": 0}).One(&mydoc); err != nil {
			log.Error(errors.Wrap(err, "Could not fetch doc from mongo"))
			return nil
		}
		key := []byte(uuid.String())
		if bytes, err := bson.Marshal(mydoc); err != nil {
			log.Error(errors.Wrap(err, "Could not serialize bson.M doc for cache"))
			return nil
		} else if err := m.doccache.Set(key, bytes, -1); err != nil {
			log.Error(errors.Wrap(err, "Could not add doc to cache"))
			return nil
		}
		return mydoc
	} else if err != nil {
		log.Error(errors.Wrap(err, "Could not fetch doc from cache"))
		return nil
	}
	var doc bson.M
	if err := bson.Unmarshal(bytes, &doc); err != nil {
		log.Error(errors.Wrap(err, "Could not unmarshal doc from cache"))
		return nil
	}
	return doc
}
