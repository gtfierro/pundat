package main

import (
	"github.com/codegangsta/cli"
	"github.com/op/go-logging"
	"os"
)

// logger
var log *logging.Logger

func init() {
	log = logging.MustGetLogger("pundat")
	var format = "%{color}%{level} %{shortfile} %{time:Jan 02 15:04:05} %{color:reset} ▶ %{message}"
	var logBackend = logging.NewLogBackend(os.Stderr, "", 0)
	logBackendLeveled := logging.AddModuleLevel(logBackend)
	logging.SetBackend(logBackendLeveled)
	logging.SetFormatter(logging.MustStringFormatter(format))
}

func main() {
	app := cli.NewApp()
	app.Name = "pundat"
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
			Usage:  "Creates a config file pundat-default.ini (default) in the current directory",
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
			},
		},
		{
			Name:   "scan",
			Usage:  "Find archivers from some base uri",
			Action: doScan,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "entity,e",
					Value:  "",
					Usage:  "The entity to use",
					EnvVar: "BW2_DEFAULT_ENTITY",
				},
			},
		},
		{
			Name:   "gettime",
			Usage:  "Convert a time expression into a Unix nano timestamp. No arguments => returns current time",
			Action: doTime,
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
