package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gtfierro/bw2util"
	bw2 "github.com/immesys/bw2bind"
	"github.com/immesys/wd"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "watcharchive"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "prefix",
		},
		cli.StringSliceFlag{
			Name: "ns,n",
		},
		cli.StringFlag{
			Name:   "agent,a",
			Value:  "127.0.0.1:28589",
			Usage:  "Local BOSSWAVE Agent",
			EnvVar: "BW2_AGENT",
		},
		cli.StringFlag{
			Name:   "entity,e",
			Value:  "",
			Usage:  "The entity to use",
			EnvVar: "BW2_DEFAULT_ENTITY",
		},
	}
	app.Action = run
	app.Run(os.Args)
}

type ArchiveRequest struct {
	URI string
	PO  string
}

func run(c *cli.Context) error {

	prefix := c.String("prefix")
	if prefix == "" {
		fmt.Println("You need to specify --prefix")
		os.Exit(1)
	}
	if !strings.HasSuffix(prefix, ".") {
		prefix += "."
	}

	_client := bw2.ConnectOrExit(c.String("agent"))
	vk := _client.SetEntityFileOrExit(c.String("entity"))
	_client.OverrideAutoChainTo(true)
	client, err := bw2util.NewClient(_client, vk)
	if err != nil {
		return err
	}

	for _, ns := range c.StringSlice("ns") {
		ns := ns
		go func(ns string) {
			subscribeURI := strings.TrimSuffix(ns, "/") + "/*/!meta/archiverequest"
			fmt.Println(subscribeURI)
			c, err := client.MultiSubscribe(subscribeURI)
			if err != nil {
				log.Fatal(err)
			}
			for msg := range c {
				parts := strings.Split(msg.URI, "/")
				key := parts[len(parts)-1]
				if key != "archiverequest" {
					continue
				}
				requests := extractRequests(msg)
				for _, req := range requests {
					go startWatcher(client, req, ns, prefix)
				}
			}
		}(ns)
	}

	x := make(chan bool)
	<-x
	return nil
}

func extractRequests(msg *bw2.SimpleMessage) []*ArchiveRequest {
	var requests []*ArchiveRequest
	for _, po := range msg.POs {
		if !po.IsTypeDF(bw2.PODFGilesArchiveRequest) {
			continue
		}
		var request = new(ArchiveRequest)
		err := po.(bw2.MsgPackPayloadObject).ValueInto(request)
		if err != nil {
			log.Println(errors.Wrap(err, "Could not parse Archive Request"))
			continue
		}
		requests = append(requests, request)
	}
	return requests
}

func startWatcher(client *bw2util.Client, req *ArchiveRequest, ns, prefix string) {
	c, err := client.Subscribe(&bw2.SubscribeParams{
		URI: req.URI,
	})
	if err != nil {
		log.Println(errors.Wrapf(err, "Could not subscribe to %s", req.URI))
		return
	}
	for msg := range c {
		po := msg.GetOnePODF(req.PO)
		if po != nil {
			uri := strings.Replace(msg.URI, "-", "_", -1)
			parts := strings.Split(uri, "/")
			parts[0] = ns
			newuri := strings.Join(parts, ".")
			if wd.RLKick(1*time.Minute, prefix+newuri, 120) {
				log.Println("kicked", msg.URI)
			}
		}
	}

}
