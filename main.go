package main

import (
	"github.com/op/go-logging"
	"github.com/urfave/cli"
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
	app.Usage = "Archiver and data access tools for BOSSWAVE and BTrDB"
	app.Version = "0.2.1"

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
				cli.BoolTFlag{
					Name:  "formattime,f",
					Usage: "If true, parse the timestamps in returned responses",
				},
				cli.StringFlag{
					Name:  "query,q",
					Value: "",
					Usage: "If specified, evaluate this query and print the results on stdout. If not specified, use the interactive query",
				},
				cli.IntFlag{
					Name:  "timeout,t",
					Value: 30,
					Usage: "Timeout in seconds to wait for a reply from the archiver",
				},
			},
		},
		{
			Name:   "scan",
			Usage:  "Find archivers from some base uri",
			Action: doScan,
			Flags: []cli.Flag{
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
			},
		},
		{
			Name:   "check",
			Usage:  "Check access to an archiver on behalf of some key",
			Action: doCheck,
			Flags: []cli.Flag{
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
				cli.StringFlag{
					Name:  "key, k",
					Usage: "The key or alias to check",
				},
				cli.StringFlag{
					Name:  "uri, u",
					Usage: "The base URI of the archiver",
				},
			},
		},
		{
			Name:   "grant",
			Usage:  "Grant access to an archiver to some key",
			Action: doGrant,
			Flags: []cli.Flag{
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
				cli.StringFlag{
					Name:   "bankroll, b",
					Value:  "",
					Usage:  "The entity to use for bankrolling",
					EnvVar: "BW2_DEFAULT_BANKROLL",
				},
				cli.StringFlag{
					Name:  "expiry",
					Usage: "Set the expiry on access to archiver measured from now e.g. 3d7h20m",
				},
				cli.StringFlag{
					Name:  "key, k",
					Usage: "The key or alias to check",
				},
				cli.StringFlag{
					Name:  "uri, u",
					Usage: "The base URI of the archiver",
				},
			},
		},
		{
			Name:   "range",
			Usage:  "Check valid ranges of access to a URI of data on behalf of some key",
			Action: doRange,
			Flags: []cli.Flag{
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
				cli.StringFlag{
					Name:   "key, k",
					Usage:  "The key or alias to check",
					EnvVar: "BW2_DEFAULT_ENTITY",
				},
				cli.StringFlag{
					Name:  "uri, u",
					Usage: "The URI we want to check ranges to",
				},
			},
		},
		// archive request commands
		{
			Name:   "listreq",
			Usage:  "List all archive requests persisted on the given URI pattern (docs: https://git.io/vDjmW)",
			Action: listArchiveRequests,
			Flags: []cli.Flag{
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
			},
		},
		{
			Name:   "rmreq",
			Usage:  "Remove archive requests specified by file (docs: https://git.io/vDjmW)",
			Action: rmConfig,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config,c",
					Usage: "Archive request YAML file",
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
			},
		},
		{
			Name:   "addreq",
			Usage:  "Load archive requests from config file (docs: https://git.io/vDjmW)",
			Action: addConfig,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "config,c",
					Value: "archive.yml",
					Usage: "Config file to parse for archive requests",
				},
				cli.StringFlag{
					Name:   "agent,a",
					Value:  "127.0.0.1:28589",
					Usage:  "Local BOSSWAVE Agent",
					EnvVar: "BW2_AGENT",
				},
				cli.StringFlag{
					Name:   "entity,e",
					EnvVar: "BW2_DEFAULT_ENTITY",
					Usage:  "The entity to use",
				},
			},
		},
		{
			Name:   "nukereq",
			Usage:  "Remove ALL archive requests attached at the given URI (docs: https://git.io/vDjmW)",
			Action: nukeArchiveRequests,
			Flags: []cli.Flag{
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
