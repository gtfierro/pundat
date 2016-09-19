package archiver

import (
	"fmt"
	"github.com/gtfierro/durandal/common"
	"github.com/gtfierro/durandal/prefix"
	"github.com/karlseguin/ccache"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"net"
	"time"
)

type mongoConfig struct {
	address *net.TCPAddr
}

type mongoStore struct {
	session      *mgo.Session
	db           *mgo.Database
	metadata     *mgo.Collection
	streams      *mgo.Collection
	mapping      *mgo.Collection
	uriUUIDCache *ccache.Cache
	pfx          *prefix.PrefixStore
}

func newMongoStore(c *mongoConfig, pfx *prefix.PrefixStore) *mongoStore {
	var err error
	m := &mongoStore{
		pfx: pfx,
	}
	log.Noticef("Connecting to MongoDB at %v...", c.address.String())
	m.session, err = mgo.Dial(c.address.String())
	if err != nil {
		log.Criticalf("Could not connect to MongoDB: %v", err)
		return nil
	}
	log.Notice("...connected!")
	// fetch/create collections and db reference
	m.db = m.session.DB("durandal")
	m.metadata = m.db.C("metadata")
	m.streams = m.db.C("streams")
	m.mapping = m.db.C("mapping")

	// add indexes. This will fail Fatal
	m.addIndexes()

	m.uriUUIDCache = ccache.New(ccache.Configure().MaxSize(10000))

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

	//index.Key = []string{"Path"}
	//index.Unique = false
	//err = m.metadata.EnsureIndex(index)
	//if err != nil {
	//	log.Fatalf("Could not create index on metadata.Path (%v)", err)
	//}

	//index.Key = []string{"SrcURI"}
	//index.Unique = false
	//err = m.metadata.EnsureIndex(index)
	//if err != nil {
	//	log.Fatalf("Could not create index on metadata.URI (%v)", err)
	//}

	//index.Key = []string{"Key"}
	//index.Unique = false
	//err = m.metadata.EnsureIndex(index)
	//if err != nil {
	//	log.Fatalf("Could not create index on metadata.Key (%v)", err)
	//}

	//// mapping indexes
	//index.Key = []string{"UUID", "URI"}
	//index.Unique = true
	//err = m.mapping.EnsureIndex(index)
	//if err != nil {
	//	log.Fatalf("Could not create index on mapping.UUID (%v)", err)
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
func (m *mongoStore) GetMetadata(VK string, tags []string, where common.Dict) ([]common.MetadataGroup, error) {
	var (
		whereClause bson.M
		_results    []bson.M
	)
	if len(where) != 0 {
		whereClause = where.ToBSON()
	}
	selectTags := bson.M{"_id": 0}
	log.Warning("WHERE", where, whereClause, selectTags)
	if err := m.metadata.Find(whereClause).Select(selectTags).All(&_results); err != nil {
		return nil, errors.Wrap(err, "Could not select tags")
	}
	log.Warning(_results)

	// serialize results and return
	var (
		results  []common.MetadataGroup
		grouping = make(map[string]*common.MetadataGroup)
		group    *common.MetadataGroup
		found    bool
	)
	for _, doc := range _results {
		record := common.RecordFromBson(doc)
		if group, found = grouping[record.UUID.String()]; !found {
			group = common.NewEmptyMetadataGroup()
			group.UUID = record.UUID
		}
		if len(tags) > 0 {
			for _, tag := range tags {
				if record.Key == tag {
					group.AddRecord(record)
				}
			}
		} else {
			group.AddRecord(record)
		}
		grouping[record.UUID.String()] = group
	}
	for _, group := range grouping {
		results = append(results, *group)
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
	log.Warning(whereClause)
	staged := m.metadata.Find(whereClause)
	if err := staged.Distinct("uuid", &_uuids); err != nil {
		return nil, errors.Wrap(err, "Could not select UUID")
	}
	log.Warning(_uuids)
	var uuids []common.UUID
	for _, uuid := range _uuids {
		uuids = append(uuids, common.ParseUUID(uuid))
	}
	log.Warning(uuids)
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
	for _, rec := range records {
		// need to "duplicate" each record by each of the streams it belongs to
		stripped := StripBangMeta(rec.SrcURI)
		uuids, err := m.pfx.GetUUIDsFromURI(stripped)
		//log.Warning("GOT UUIDS", uuids)
		if err != nil {
			return err
		}
		for _, u := range uuids {
			rec.UUID = u
			//log.Debugf("Inserting %+v", rec)
			if _, err := m.metadata.Upsert(bson.M{"Key": rec.Key, "SrcURI": rec.SrcURI, "UUID": rec.UUID}, rec); !mgo.IsDup(err) {
				return err
			}
		}
	}
	return nil
}

func (m *mongoStore) RemoveMetadata(VK string, tags []string, where common.Dict) error {
	return nil
}

func (m *mongoStore) MapURItoUUID(uri, ponum, valueExpr string, uuid common.UUID) error {
	err := m.mapping.Insert(bson.M{"URI": uri, "PO": ponum, "valueExpr": valueExpr, "UUID": uuid})
	m.uriUUIDCache.Set(uri, uuid, time.Minute*10)
	if mgo.IsDup(err) {
		return nil
	}
	return err
}

func (m *mongoStore) URItoUUID(uri string) (common.UUID, error) {
	item, err := m.uriUUIDCache.Fetch(uri, time.Minute*10, func() (interface{}, error) {
		var (
			uuid common.UUID
		)
		err := m.mapping.Find(bson.M{"URI": uri}).Select(bson.M{"uuid": 1}).One(&uuid)
		if err != nil {
			return nil, nil
		}
		return uuid, nil
	})
	item.Extend(10 * time.Minute)
	if item.Value() == nil {
		return nil, errors.New(fmt.Sprintf("No UUID for URI %s", uri))
	}
	return item.Value().(common.UUID), err
}
