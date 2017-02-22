package main

import (
	"fmt"
	"github.com/gtfierro/pundat/requests"
	bw2 "github.com/immesys/bw2bind"
	"github.com/urfave/cli"
)

func listArchiveRequests(c *cli.Context) error {
	bw2.SilenceLog()
	bwclient := bw2.ConnectOrExit(c.String("agent"))
	bwclient.SetEntityFileOrExit(c.String("entity"))
	bwclient.OverrideAutoChainTo(true)
	if c.NArg() == 0 {
		log.Fatal("Need URI")
	}
	uri := c.Args().Get(0)
	if uri == "" {
		log.Fatal("Need URI")
	}
	archive_requests, err := requests.GetArchiveRequests(bwclient, uri)
	if err != nil {
		log.Fatal(err)
	}
	for _, req := range archive_requests {
		fmt.Println("---------------")
		req.Dump()
	}
	return nil
}

func rmConfig(c *cli.Context) error {
	bw2.SilenceLog()
	bwclient := bw2.ConnectOrExit(c.String("agent"))
	bwclient.SetEntityFileOrExit(c.String("entity"))
	bwclient.OverrideAutoChainTo(true)
	return requests.RemoveArchiveRequestsFromConfig(bwclient, c.String("config"))
}

func addConfig(c *cli.Context) error {
	bw2.SilenceLog()
	bwclient := bw2.ConnectOrExit(c.String("agent"))
	bwclient.SetEntityFileOrExit(c.String("entity"))
	bwclient.OverrideAutoChainTo(true)
	return requests.AddArchiveRequestsFromConfig(bwclient, c.String("config"), c.String("uri"))
}

func nukeArchiveRequests(c *cli.Context) error {
	bw2.SilenceLog()
	bwclient := bw2.ConnectOrExit(c.String("agent"))
	bwclient.SetEntityFileOrExit(c.String("entity"))
	bwclient.OverrideAutoChainTo(true)
	if c.NArg() == 0 {
		log.Fatal("Need URI")
	}
	uri := c.Args().Get(0)
	if uri == "" {
		log.Fatal("Need URI")
	}
	return requests.RemoveAllArchiveRequests(bwclient, uri)
}
