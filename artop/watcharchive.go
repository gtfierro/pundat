package main

import (
	"container/ring"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/gtfierro/bw2util"
	bw2 "github.com/immesys/bw2bind"
	"github.com/immesys/wd"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

var removeSuffix = regexp.MustCompile("/signal/(.*)")

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

func fixURI(uri string) string {
	uri = strings.Replace(uri, "-", "_", -1)
	uri = strings.Replace(uri, "+", "_", -1)
	uri = strings.Replace(uri, "*", "_", -1)
	uri = strings.ToLower(uri)
	uri = removeSuffix.ReplaceAllString(uri, "")
	return uri
}

func startWatcher(client *bw2util.Client, req *ArchiveRequest, ns, prefix string) {
	//TODO: adapt to reporting rate. RLKick is 2* the reporting interval, with minimum of 2 minutes

	// sliding window of report times
	var reportWindow = ring.New(10)

	c, err := client.Subscribe(&bw2.SubscribeParams{
		URI: req.URI,
	})
	if err != nil {
		log.Println(errors.Wrapf(err, "Could not subscribe to %s", req.URI))
		return
	}
	// clean uri
	log.Println(req.URI)

	parts := strings.Split(req.URI, "/")
	parts[0] = ns
	uri := fixURI(prefix + strings.Join(parts, "."))

	log.Println(uri)
	wd.Kick(uri, 30*60) // 30 min
	for msg := range c {
		po := msg.GetOnePODF(req.PO)

		reportWindow.Value = time.Now().UnixNano()
		reportWindow = reportWindow.Next()

		avgDiff := int64(0)
		num := 0
		prev := int64(0)
		avg := func(v interface{}) {
			if v != nil {
				if prev > 0 {
					avgDiff += (v.(int64) - prev)
					num += 1
				}
				prev = v.(int64)
			}
		}

		reportWindow.Do(avg)
		interval := time.Duration(int64(float64(avgDiff)/float64(num))) * time.Nanosecond
		if interval.Seconds() < 60 {
			interval = 60 * time.Second
		}

		if po != nil && num > 1 {
			if wd.RLKick(interval, uri, int(interval.Seconds()*2)) {
				log.Println("kicked", uri, interval, reportWindow.Len())
			}
		}
	}

}
