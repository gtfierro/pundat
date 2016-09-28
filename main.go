package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/gtfierro/bwquery/api"
	"github.com/gtfierro/durandal/archiver"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"gopkg.in/readline.v1"
	"os"
	"os/user"
	"runtime"
	"time"
)

// logger
var log *logging.Logger

func init() {
	log = logging.MustGetLogger("durandal")
	var format = "%{color}%{level} %{shortfile} %{time:Jan 02 15:04:05} %{color:reset} ▶ %{message}"
	var logBackend = logging.NewLogBackend(os.Stderr, "", 0)
	logBackendLeveled := logging.AddModuleLevel(logBackend)
	logging.SetBackend(logBackendLeveled)
	logging.SetFormatter(logging.MustStringFormatter(format))
}

func startArchiver(c *cli.Context) error {
	config := archiver.LoadConfig(c.String("config"))
	if config.Archiver.PeriodicReport {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				log.Infof("Number of active goroutines %v", runtime.NumGoroutine())
			}
		}()
	}
	a := archiver.NewArchiver(config)
	a.Serve()
	return nil
}

func makeConfig(c *cli.Context) error {
	filename := c.String("file")
	if filename == "" {
		filename = "durandal-default.ini"
	}
	f, err := os.Create(filename)
	if err != nil {
		return errors.Wrapf(err, "Could not create file %s", filename)
	}
	fmt.Fprintln(f, "[Archiver]")
	fmt.Fprintln(f, "PeriodicReport = true")
	fmt.Fprintln(f, "BlockExpiry = 1")
	fmt.Fprintln(f, "")
	fmt.Fprintln(f, "[BOSSWAVE]")
	fmt.Fprintln(f, "Address = 0.0.0.0:28589")
	fmt.Fprintln(f, "Entityfile = myentity.ent")
	fmt.Fprintln(f, "Namespace = scratch.ns")
	fmt.Fprintln(f, "DeployNS = scratch.ns")
	fmt.Fprintln(f, "ListenNS = scratch.ns")
	fmt.Fprintln(f, "")
	fmt.Fprintln(f, "[Metadata]")
	fmt.Fprintln(f, "Address = 0.0.0.0:27017")
	fmt.Fprintln(f, "")
	fmt.Fprintln(f, "[BtrDB]")
	fmt.Fprintln(f, "Address = 0.0.0.0:4410")
	return f.Sync()
}

func doIQuery(c *cli.Context) error {
	client := bw2.ConnectOrExit("")
	vk := client.SetEntityFileOrExit(c.String("entity"))
	client.OverrideAutoChainTo(true)
	API := api.NewAPI(client, vk, c.String("archiver"))

	res, err := client.Query(&bw2.QueryParams{
		URI: c.String("archiver") + "/s.giles/!meta/lastalive",
	})
	if err != nil {
		return err
	}
	for msg := range res {
		var md map[string]interface{}
		po := msg.GetOnePODF(bw2.PODFMaskSMetadata)
		if err := po.(bw2.MsgPackPayloadObject).ValueInto(&md); err != nil {
			log.Error(errors.Wrap(err, "Could not decode lastalive time"))
		} else {
			//2016-09-16 10:41:40.818797445 -0700 PDT
			lastalive, err := time.Parse("2006-01-02 15:04:05 -0700 MST", md["val"].(string))
			if err != nil {
				log.Error(errors.Wrap(err, "Could not decode lastalive time"))
			}
			ago := time.Since(lastalive)
			if ago.Minutes() > time.Duration(5*time.Minute).Minutes() {
				log.Errorf("Archiver at %s last alive at %v (%v ago)", c.String("archiver"), lastalive, ago)
			} else {
				log.Infof("Archiver at %s last alive at %v (%v ago)", c.String("archiver"), lastalive, ago)
			}
		}
	}

	currentUser, err := user.Current()
	if err != nil {
		return err
	}

	completer := readline.NewPrefixCompleter(
		readline.PcItem("select",
			readline.PcItem("data",
				readline.PcItem("in"),
				readline.PcItem("before"),
				readline.PcItem("after"),
			),
			readline.PcItem("Metadata/"),
			readline.PcItem("distinct",
				readline.PcItem("Metadata/"),
				readline.PcItem("uuid/"),
			),
			readline.PcItem("uuid"),
		),
	)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:       "(bwquery)>",
		AutoComplete: completer,
		HistoryFile:  currentUser.HomeDir + "/.bwquery",
	})
	if err != nil {
		return err
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			fmt.Println(err)
			break
		}
		API.Query(line)
	}
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "Durandal"
	app.Version = "aleph.1"

	app.Commands = []cli.Command{
		{
			Name:   "archiver",
			Usage:  "Start the archiver",
			Action: startArchiver,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config,c",
					Usage: "Configuration file",
				},
			},
		},
		{
			Name:   "mkconfig",
			Usage:  "Creates a config file durandal-default.ini (default) in the current directory",
			Action: makeConfig,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "file,f",
					Usage: "Name of the config file",
				},
			},
		},
		{
			Name:   "query",
			Usage:  "Evaluate query interactively",
			Action: doIQuery,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "entity,e",
					Value:  "",
					Usage:  "The entity to use",
					EnvVar: "BW2_DEFAULT_ENTITY",
				},
				cli.StringFlag{
					Name:  "archiver,a",
					Value: "scratch.ns",
					Usage: "REQUIRED. The base URI of the archiver you want to query",
				},
			},
		},
	}
	app.Run(os.Args)
}

/*
NOTES:
- query language parsing has been ported over
- first challenge is to get metadata queries working
Questions:
1. Do we save the whole timeseries of metadata records?
    - if we only associate the most recent metadata record w/ a timeseries,
      then someone who *could* query in the past might not be able to if the md tag
      is deleted, but they may have lost permission before the tag was deleted. Now,
      they know that the tag no longer applies to the stream.
    - the solution here is likely to have the full record of all metadata values,
      then build a DOT to ALL of them and filter the list of the most recent unique
      (key, srcuri) pairs. this is the "view" of metadata as they are allowed to see it
    - TODO: really want to create some sort of model or formalism or notes of how
      metadata and permissions and time all play together. What are all the edge cases of
      this? We want to plan for those

TODO:
- finish implementing metadata store:
    - involves BASIC benchmarks
    - implement the DOT stuff
    - test the DOT stuff
- port over the archiver query interface:
    - the archiver API used by the language
*/
