package archiver

import (
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
	namespace string
}

func NewArchiver(c *Config) (a *Archiver) {
	a = &Archiver{}

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

	// TODO: listen for queries

	// TODO: create the View to listen for the archive requests

	return a
}
