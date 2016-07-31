package archiver

//import (
//	"github.com/gtfierro/durandal/common"
//	"gopkg.in/mgo.v2"
//	"gopkg.in/mgo.v2/bson"
//)
//
//type mongoStore struct {
//	session  *mgo.Session
//	db       *mgo.Database
//	metadata *mgo.Collection
//}
//
//func newMongoStore(c *mongoConfig) *mongoStore {
//	var err error
//	m := &mongoStore{}
//	log.Noticef("Connecting to MongoDB at %v...", c.address.String())
//	m.session, err = mgo.Dial(c.address.String())
//	if err != nil {
//		log.Criticalf("Could not connect to MongoDB: %v", err)
//		return nil
//	}
//	log.Notice("...connected!")
//	// fetch/create collections and db reference
//	m.db = m.session.DB("durandal")
//	m.metadata = m.db.C("metadata")
//
//	// add indexes. This will fail Fatal
//	m.addIndexes()
//
//	return m
//}
//
//func (m *mongoStore) addIndexes() {
//	var err error
//	// create indexes
//	index := mgo.Index{
//		Key:        []string{"UUID"},
//		Unique:     true,
//		DropDups:   false,
//		Background: false,
//		Sparse:     false,
//	}
//	err = m.metadata.EnsureIndex(index)
//	if err != nil {
//		log.Fatalf("Could not create index on metadata.UUID (%v)", err)
//	}
//
//	index.Key = []string{"Path"}
//	index.Unique = false
//	err = m.metadata.EnsureIndex(index)
//	if err != nil {
//		log.Fatalf("Could not create index on metadata.Path (%v)", err)
//	}
//
//	index.Key = []string{"URI"}
//	index.Unique = false
//	err = m.metadata.EnsureIndex(index)
//	if err != nil {
//		log.Fatalf("Could not create index on metadata.URI (%v)", err)
//	}
//
//	index.Key = []string{"Key"}
//	index.Unique = false
//	err = m.metadata.EnsureIndex(index)
//	if err != nil {
//		log.Fatalf("Could not create index on metadata.Key (%v)", err)
//	}
//
//}
//
//func (m *mongoStore) GetUnitOfTime(VK string, uuid common.UUID) (common.UnitOfTime, error) {
//	uot = common.UOT_S
//	query := m.metadata.Find(bson.M{"uuid": uuid}).Select(bson.M{"UnitofTime": 1})
//	if c, err = query.Count(); err != nil {
//		return
//	} else if c == 0 {
//		err = fmt.Errorf("no stream named %v", uuid)
//		return
//	}
//	err = query.One(&res)
//	if entry, found := res.(bson.M)["UnitofTime"]; found {
//		if uotInt, isInt := entry.(int); isInt {
//			uot = common.UnitOfTime(uotInt)
//		} else {
//			err = fmt.Errorf("Invalid UnitOfTime retrieved? %v", entry)
//			return
//		}
//		uot = common.UnitOfTime(entry.(int))
//		if uot == 0 {
//			uot = common.UOT_S
//		}
//	}
//	return uot, nil
//}
//func (m *mongoStore) GetMetadata(VK string, tags []string, where common.Dict) ([]*common.MetadataGroup, error) {
//}
//func (m *mongoStore) GetDistinct(VK string, tag string, where common.Dict) ([]*common.MetadataRecord, error) {
//}
//func (m *mongoStore) SaveMetadata(VK string, records []*common.MetadataRecord) error   {}
//func (m *mongoStore) RemoveMetadata(VK string, tags []string, where common.Dict) error {}
