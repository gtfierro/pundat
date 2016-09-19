package archiver

import (
	"fmt"
	"github.com/gtfierro/durandal/common"
	"github.com/gtfierro/durandal/prefix"
	"github.com/gtfierro/durandal/querylang"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"net"
	"os"
)

// logger
var log *logging.Logger

// set up logging facilities
func init() {
	log = logging.MustGetLogger("archiver")
	var format = "%{color}%{level} %{shortfile} %{time:Jan 02 15:04:05} %{color:reset} ▶ %{message}"
	var logBackend = logging.NewLogBackend(os.Stderr, "", 0)
	logBackendLeveled := logging.AddModuleLevel(logBackend)
	logging.SetBackend(logBackendLeveled)
	logging.SetFormatter(logging.MustStringFormatter(format))
}

type Archiver struct {
	bw        *bw2.BW2Client
	vk        string
	MD        MetadataStore
	TS        TimeseriesStore
	pfx       *prefix.PrefixStore
	DM        *DotMaster
	svc       *bw2.Service
	iface     *bw2.Interface
	vm        *viewManager
	ms        *metadatasubscriber
	qp        *querylang.QueryProcessor
	namespace string
	config    *Config
	stop      chan bool
}

func NewArchiver(c *Config) (a *Archiver) {
	a = &Archiver{
		config: c,
		stop:   make(chan bool),
	}
	// setup prefix store
	a.pfx = prefix.NewPrefixStore(".pfx.db")

	// setup metadata
	mongoaddr, err := net.ResolveTCPAddr("tcp4", c.Metadata.Address)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "Could not resolve Metadata address %s", c.Metadata.Address))
	}
	a.MD = newMongoStore(&mongoConfig{address: mongoaddr}, a.pfx)

	btrdbaddr, err := net.ResolveTCPAddr("tcp4", c.BtrDB.Address)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "Could not resolve BtrDB address %s", c.BtrDB.Address))
	}
	a.TS = newBtrIface(&btrdbConfig{address: btrdbaddr})

	// setup bosswave
	a.namespace = c.BOSSWAVE.Namespace
	a.bw = bw2.ConnectOrExit(c.BOSSWAVE.Address)
	a.bw.OverrideAutoChainTo(true)
	a.vk = a.bw.SetEntityFileOrExit(c.BOSSWAVE.Entityfile)
	//a.svc = a.bw.RegisterService(a.namespace, "s.giles")
	//a.iface = a.svc.RegisterInterface("0", "i.archiver")

	// setup dot master
	a.DM = NewDotMaster(a.bw, c.Archiver.BlockExpiry)

	a.ms = newMetadataSubscriber(a.bw, a.MD, a.pfx)

	// setup view manager
	a.vm = newViewManager(a.bw, a.MD, a.TS, a.pfx, a.ms)

	// TODO: listen for queries
	a.qp = querylang.NewQueryProcessor()

	// TODO: create the View to listen for the archive requests
	a.svc = a.bw.RegisterService(c.BOSSWAVE.DeployNS, "s.giles")
	a.iface = a.svc.RegisterInterface("0", "i.archiver")
	queryChan, err := a.bw.Subscribe(&bw2.SubscribeParams{
		URI: a.iface.SlotURI("query"),
	})
	if err != nil {
		log.Error(errors.Wrap(err, "Could not subscribe"))
	}
	log.Noticef("Listening on %s", a.iface.SlotURI("query"))
	log.Noticef("Listening on %s", a.iface.SlotURI("subscribe"))
	common.NewWorkerPool(queryChan, a.listenQueries, 1000).Start()

	return a
}

func (a *Archiver) Serve() {
	for _, namespace := range a.config.BOSSWAVE.ListenNS {
		a.vm.subscribeNamespace(namespace)
	}
	<-a.stop
}

func (a *Archiver) Stop() {
	a.stop <- true
}

func (a *Archiver) listenQueries(msg *bw2.SimpleMessage) {
	var (
		// the publisher of the message. We incorporate this into the signal URI
		fromVK string
		// the computed signal based on the VK and query nonce
		signalURI string
		// query message
		query KeyValueQuery
	)
	fromVK = msg.From
	po := msg.GetOnePODF(bw2.PODFGilesKeyValueQuery)
	if po == nil { // no query found
		return
	}

	if obj, ok := po.(bw2.MsgPackPayloadObject); !ok {
		log.Error("Received query was not msgpack")
	} else if err := obj.ValueInto(&query); err != nil {
		log.Error(errors.Wrap(err, "Could not unmarshal received query"))
		return
	}

	signalURI = fmt.Sprintf("%s,queries", fromVK[:len(fromVK)-1])

	log.Infof("Got query %+v", query)
	mdRes, tsRes, err := a.HandleQuery(fromVK, query.Query)
	if err != nil {
		msg := QueryError{
			Query: query.Query,
			Nonce: query.Nonce,
			Error: err.Error(),
		}
		po, _ := bw2.CreateMsgPackPayloadObject(bw2.PONumGilesQueryError, msg)
		log.Error(errors.Wrap(err, "Error evaluating query"))
		if err := a.iface.PublishSignal(signalURI, po); err != nil {
			log.Error(errors.Wrap(err, "Error sending response"))
		}
	}

	var reply []bw2.PayloadObject

	log.Infof("Got Metadata %+v", mdRes)
	log.Infof("Got Timeseries %+v", tsRes)
	metadataPayload := POsFromMetadataGroup(query.Nonce, mdRes)
	reply = append(reply, metadataPayload)

	timeseriesPayload := POsFromTimeseriesGroup(query.Nonce, tsRes)
	reply = append(reply, timeseriesPayload)

	log.Debugf("Reply on %s: %d", a.iface.SignalURI(signalURI), len(reply))

	if err := a.iface.PublishSignal(signalURI, reply...); err != nil {
		log.Error(errors.Wrap(err, "Error sending response"))
	}
}

func (a *Archiver) HandleQuery(vk, query string) (mdResult []common.MetadataGroup, tsResult []common.Timeseries, err error) {
	parsed := a.qp.Parse(query)
	if parsed.Err != nil {
		err = fmt.Errorf("Error (%v) in query \"%v\" (error at %v)\n", parsed.Err, query, parsed.ErrPos)
		return
	}

	switch parsed.QueryType {
	case querylang.SELECT_TYPE:
		if parsed.Distinct {
			err = fmt.Errorf("DISTINCT not implemented yet sorry")
			return
		}
		params := parsed.GetParams().(*common.TagParams)
		mdResult, err = a.SelectTags(vk, params)
		return
	case querylang.DATA_TYPE:
		params := parsed.GetParams().(*common.DataParams)
		//if params.IsStatistical || params.IsWindow {
		//	return a.SelectStatisticalData(params)
		//}
		switch parsed.Data.Dtype {
		case querylang.IN_TYPE:
			log.Warning("SELECT DATA RANGE", params)
			tsResult, err = a.SelectDataRange(params)
			return
		case querylang.BEFORE_TYPE:
			log.Warning("SELECT DATA BEFORE", params)
			tsResult, err = a.SelectDataBefore(params)
			return
			//case querylang.AFTER_TYPE:
			//	return a.SelectDataAfter(params)
		}
	}

	return
}

//func (a *Archiver) SelectTags(params *common.TagParams) (QueryResult, error) {
//func (a *Archiver) DistinctTag(params *common.DistinctParams) (QueryResult, error) {
//func (a *Archiver) SelectDataRange(params *common.DataParams) (common.SmapMessageList, error) {
//func (a *Archiver) SelectDataBefore(params *common.DataParams) (result common.SmapMessageList, err error) {
//func (a *Archiver) SelectDataAfter(params *common.DataParams) (result common.SmapMessageList, err error) {
//func (a *Archiver) SelectStatisticalData(params *common.DataParams) (result common.SmapMessageList, err error) {
//func (a *Archiver) DeleteData(params *common.DataParams) (err error) {
