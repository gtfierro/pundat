package archiver

import (
	"github.com/gtfierro/durandal/common"
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
	var format = "%{color}%{level} %{time:Jan 02 15:04:05} %{shortfile}%{color:reset} ▶ %{message}"
	var logBackend = logging.NewLogBackend(os.Stderr, "", 0)
	logBackendLeveled := logging.AddModuleLevel(logBackend)
	logging.SetBackend(logBackendLeveled)
	logging.SetFormatter(logging.MustStringFormatter(format))
}

type Archiver struct {
	bw        *bw2.BW2Client
	vk        string
	MD        MetadataStore
	DM        *DotMaster
	svc       *bw2.Service
	iface     *bw2.Interface
	vm        *viewManager
	namespace string
	config    *Config
	stop      chan bool
}

func NewArchiver(c *Config) (a *Archiver) {
	a = &Archiver{
		config: c,
		stop:   make(chan bool),
	}

	// setup metadata
	mongoaddr, err := net.ResolveTCPAddr("tcp4", c.Metadata.Address)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "Could not resolve Metadata address %s", c.Metadata.Address))
	}
	a.MD = newMongoStore(&mongoConfig{address: mongoaddr})

	// setup bosswave
	a.namespace = c.BOSSWAVE.Namespace
	a.bw = bw2.ConnectOrExit(c.BOSSWAVE.Address)
	a.bw.OverrideAutoChainTo(true)
	a.vk = a.bw.SetEntityFileOrExit(c.BOSSWAVE.Entityfile)
	a.svc = a.bw.RegisterService(a.namespace, "s.giles")
	a.iface = a.svc.RegisterInterface("0", "i.archiver")

	// setup dot master
	a.DM = NewDotMaster(a.bw, c.Archiver.BlockExpiry)

	// setup view manager
	a.vm = newViewManager(a.bw)

	// TODO: listen for queries

	// TODO: create the View to listen for the archive requests
	a.svc = a.bw.RegisterService(c.BOSSWAVE.DeployNS, "s.giles")
	a.iface = a.svc.RegisterInterface("_", "i.archiver")
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
}
