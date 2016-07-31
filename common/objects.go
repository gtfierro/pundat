package common

//TODO: conform to the Swift API http://docs.ceph.com/docs/master/radosgw/swift/
type Object struct {
	// uint64 timestamp
	Time uint64
	UoT  UnitOfTime
	// value associated with this timestamp
	Value []byte
}

type ObjectList struct {
	Records []*Object
}
