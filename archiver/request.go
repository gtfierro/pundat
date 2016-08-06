package archiver

import (
	"fmt"
	"github.com/gtfierro/durandal/common"
	ob "github.com/gtfierro/giles2/objectbuilder"
	"github.com/satori/go.uuid"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"sync"
	"time"
)

var NAMESPACE_UUID = uuid.FromStringOrNil("b26d2e62-333e-11e6-b557-0cc47a0f7eea")

// This object is a set of instructions for how to create an archivable message
// from some received PayloadObject, though really this should be able to
// operate on any object. Each ArchiveRequest acts as a translator for received
// messages into a single timeseries stream
type ArchiveRequest struct {
	sync.RWMutex
	// AUTOPOPULATED. The entity that requested the URI to be archived.
	FromVK string
	// OPTIONAL. the URI to subscribe to. Requires building a chain on the URI
	// from the .FromVK field. If not provided, uses the base URI of where this
	// ArchiveRequest was stored. For example, if this request was published
	// on <uri>/!meta/giles, then if the URI field was elided it would default
	// to <uri>.
	URI string
	// Extracts objects of the given Payload Object type from all messages
	// published on the URI. If elided, operates on all PO types.
	PO int
	// OPTIONAL. If provided, this is an objectbuilder expr to extract the stream UUID.  If not
	// provided, then a UUIDv3 with NAMESPACE_UUID and the URI, PO type and
	// Value are used.
	UUID string
	// the real UUID when we get it
	uuidActual common.UUID
	uuid       []ob.Operation
	// expression determining how to extract the value from the received
	// message
	Value string
	value []ob.Operation
	// OPTIONAL. Expression determining how to extract the value from the
	// received message. If not included, it uses the time the message was
	// received on the server.
	Time string
	time []ob.Operation
	// OPTIONAL. Golang time parse string
	TimeParse string

	// OPTIONAL. Defaults to true. If true, the archiver will call bw2bind's "GetMetadata" on the archived URI,
	// which inherits metadata from each of its components
	InheritMetadata bool
	// OPTIONAL. a list of base URIs to scan for metadata. If `<uri>` is provided, we
	// scan `<uri>/!meta/+` for metadata keys/values
	MetadataURIs []string

	// OPTIONAL. a URI terminating in a metadata key that contains some kv
	// structure of metadata, for example `/a/b/c/!meta/metadatahere`
	MetadataBlock string

	// OPTIONAL. a ObjectBuilder expression to search in the current message
	// for metadata
	MetadataExpr string
	metadataExpr []ob.Operation

	// to cancel an archive request, send on this channel
	cancel chan bool
}

// Print the parameters
func (req *ArchiveRequest) Dump() {
	fmt.Printf("PublishedBy: %s\n", req.FromVK)
	fmt.Printf("Archiving: %s\n", req.URI)
	if req.PO > 0 {
		fmt.Printf("Extracting PO: %s\n", bw2.PONumDotForm(req.PO))
	} else {
		fmt.Printf("Extracts all POs\n")
	}
	if len(req.uuidActual) > 0 {
		fmt.Printf("Stream UUID: %s\n", req.uuidActual)
	} else {
		fmt.Printf("UUID Expression: %s\n", req.UUID)
	}

	fmt.Printf("Value Expr: %s\n", req.Value)

	if req.Time != "" {
		fmt.Printf("Time Expr: %s\n", req.Time)
		fmt.Printf("Parse Time: %s\n", req.TimeParse)
	} else {
		fmt.Printf("Using server timestamps\n")
	}

	fmt.Println("Metadata:")
	if req.InheritMetadata {
		fmt.Println("Inheriting metadata from URI prefixes")
	}
	if len(req.MetadataURIs) > 0 {
		for _, uri := range req.MetadataURIs {
			fmt.Printf(" Metadata from URI %s\n", uri)
		}
	}
	if req.MetadataBlock != "" {
		fmt.Printf(" Metadata block uri: %s\n", req.MetadataBlock)
	}
	if req.MetadataExpr != "" {
		fmt.Printf(" Metadata Expr: %s\n", req.MetadataExpr)
	}
}

// Creates a hash of this object that is unique to its parameters. We will use the URI, PO, UUID and Value
func (req *ArchiveRequest) Hash() string {
	return req.URI + bw2.PONumDotForm(req.PO) + req.UUID + req.Value
}

func (req *ArchiveRequest) getTime(thing interface{}) uint64 {
	if len(req.time) == 0 {
		return uint64(time.Now().UnixNano())
	}
	timeString, ok := ob.Eval(req.time, thing).(string)
	if ok {
		parsedTime, err := time.Parse(req.TimeParse, timeString)
		if err != nil {
			return uint64(time.Now().UnixNano())
		}
		return uint64(parsedTime.UnixNano())
	}
	return uint64(time.Now().UnixNano())
}

// Returns true if the two ArchiveRequests are equal
func (req *ArchiveRequest) Equals(other *ArchiveRequest) bool {
	return (req != nil && other != nil) &&
		(req.URI == other.URI) &&
		(req.PO == other.PO) &&
		(req.UUID == other.UUID) &&
		(req.Value == other.Value) &&
		(req.Time == other.Time) &&
		(req.TimeParse == other.TimeParse) &&
		(req.InheritMetadata == other.InheritMetadata) &&
		(compareStringSliceAsSet(req.MetadataURIs, other.MetadataURIs)) &&
		(req.MetadataBlock == other.MetadataBlock) &&
		(req.MetadataExpr == other.MetadataExpr)
}

//func (req *ArchiveRequest) GetSmapMessage(thing interface{}) *common.MetadataGroup {
//
//	value := ob.Eval(req.value, thing)
//	switch t := value.(type) {
//	case int64:
//		rdg.Value = float64(t)
//	case uint64:
//		rdg.Value = float64(t)
//	case float64:
//		rdg.Value = t
//	}
//
//	rdg.Time = req.getTime(thing)
//
//	if len(req.uuid) > 0 && req.uuidActual == "" {
//		req.uuidActual = common.UUID(ob.Eval(req.uuid, thing).(string))
//	} else if req.uuidActual == "" {
//		req.uuidActual = common.UUID(req.UUID)
//	}
//	msg.UUID = req.uuidActual
//	msg.Path = req.URI + "/" + req.Value
//	msg.Readings = []common.Reading{rdg}
//
//	if len(req.metadataExpr) > 0 {
//		msg.Metadata = make(common.Dict)
//		msg.Properties = new(common.SmapProperties)
//		if md, ok := ob.Eval(req.metadataExpr, thing).(map[string]interface{}); ok {
//			for k, v := range md {
//				val := fmt.Sprintf("%s", v)
//				if k == "UnitofTime" {
//					msg.Properties.UnitOfTime, _ = common.ParseUOT(val)
//				} else if k == "UnitofMeasure" {
//					msg.Properties.UnitOfMeasure = val
//				}
//				msg.Metadata[k] = val
//			}
//		}
//	}
//
//	return msg
//}
