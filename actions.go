package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/gtfierro/pundat/archiver"
	"github.com/gtfierro/pundat/client"
	"github.com/mgutz/ansi"
	"github.com/pkg/errors"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"gopkg.in/readline.v1"
	"os"
	"os/user"
	"runtime"
	"strings"
	"time"
)

func getArchiverAlive(msg *bw2.SimpleMessage) (string, time.Time, error) {
	var (
		md        map[string]interface{}
		uri       string
		lastalive time.Time
		err       error
	)
	po := msg.GetOnePODF(bw2.PODFMaskSMetadata)
	uri = strings.TrimSuffix(msg.URI, "/s.giles/!meta/lastalive")
	if err := po.(bw2.MsgPackPayloadObject).ValueInto(&md); err != nil {
		log.Error(errors.Wrap(err, "Could not decode lastalive time"))
	} else {
		//2016-09-16 10:41:40.818797445 -0700 PDT
		lastalive, err = time.Parse("2006-01-02 15:04:05 -0700 MST", md["val"].(string))
		if err != nil {
			log.Error(errors.Wrap(err, "Could not decode lastalive time"))
		}
	}
	return uri, lastalive, err
}

func scan(uri, entity string) ([]string, []time.Time, error) {
	var found []string
	var times []time.Time

	client := bw2.ConnectOrExit("")
	client.SetEntityFileOrExit(entity)
	client.OverrideAutoChainTo(true)

	uri = strings.TrimRight(uri, "/*+")
	stuff := strings.Split(uri, "/")
	namespace := stuff[0]

	res, err := client.Query(&bw2.QueryParams{
		URI: uri + "/*/s.giles/!meta/lastalive",
	})
	if err != nil {
		return found, times, err
	}
	for msg := range res {
		uri, alive, err := getArchiverAlive(msg)
		if err != nil {
			log.Error(errors.Wrapf(err, "Could not retrive archiver last alive time at %s", uri))
			continue
		}
		editIndex := strings.Index(uri, "/")
		if editIndex > 0 {
			uri = namespace + "/" + uri[editIndex+1:]
		} else {
			uri = namespace
		}
		found = append(found, uri)
		times = append(times, alive)
	}
	return found, times, nil
}

func startArchiver(c *cli.Context) error {
	config := archiver.LoadConfig(c.String("config"))
	if config.Archiver.PeriodicReport {
		go func() {
			for {
				time.Sleep(10 * time.Second)
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
		filename = "pundat-default.ini"
	}
	f, err := os.Create(filename)
	if err != nil {
		return errors.Wrapf(err, "Could not create file %s", filename)
	}
	fmt.Fprintln(f, "[Archiver]")
	fmt.Fprintln(f, "PeriodicReport = true")
	fmt.Fprintln(f, "BlockExpiry = 10s")
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
	fmt.Fprintln(f, "CollectionPrefix = pundat")
	fmt.Fprintln(f, "")
	fmt.Fprintln(f, "[BtrDB]")
	fmt.Fprintln(f, "Address = 0.0.0.0:4410")
	return f.Sync()
}

func doIQuery(c *cli.Context) error {
	bwclient := bw2.ConnectOrExit("")
	vk := bwclient.SetEntityFileOrExit(c.String("entity"))
	bwclient.OverrideAutoChainTo(true)

	if c.NArg() == 0 {
		return errors.New("Need to specify a namespace or URI prefix of an archiver (can use 'pundat scan' to help)")
	}
	archiverURI := c.Args().Get(0)

	pc, err := client.NewPundatClient(bwclient, vk, archiverURI)
	if err != nil {
		return err
	}

	res, err := bwclient.Query(&bw2.QueryParams{
		URI: archiverURI + "/s.giles/!meta/lastalive",
	})
	if err != nil {
		log.Error(err)
	} else {
		for msg := range res {
			uri, lastalive, err := getArchiverAlive(msg)
			if err != nil {
				log.Error(errors.Wrapf(err, "Could not retrive archiver last alive time at %s", uri))
				continue
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
		Prompt:       "(pundat)>",
		AutoComplete: completer,
		HistoryFile:  currentUser.HomeDir + "/.pundat_query_history",
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
		md, ts, ch, err := pc.Query(line)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if !md.IsEmpty() {
			fmt.Println(md.Dump())
		}
		if !ts.IsEmpty() {
			fmt.Println(ts.Dump())
		}
		if !ch.IsEmpty() {
			fmt.Println(ch.Dump())
		}
	}
	return nil
}

func doScan(c *cli.Context) error {

	if c.NArg() == 0 {
		return errors.New("Need to specify a namespace or URI prefix to scan")
	}

	archivers, times, err := scan(c.Args().Get(0), c.String("entity"))
	if err != nil {
		return err
	}

	for i := 0; i < len(archivers); i++ {
		alive := times[i]
		ago := time.Since(alive)
		uri := archivers[i]
		oldColor := ansi.ColorFunc("red")
		newColor := ansi.ColorFunc("green+b")
		if ago.Minutes() > time.Duration(5*time.Minute).Minutes() {
			fmt.Println(oldColor("Found Archiver at:"))
			fmt.Println(oldColor(fmt.Sprintf("     URI        -> %s", uri)))
			fmt.Println(oldColor(fmt.Sprintf("     Last Alive -> %v (%v ago)", alive, ago)))
			fmt.Println()
		} else {
			fmt.Println(newColor("Found Archiver at:"))
			fmt.Println(newColor(fmt.Sprintf("     URI        -> %s", uri)))
			fmt.Println(newColor(fmt.Sprintf("     Last Alive -> %v (%v ago)", alive, ago)))
			fmt.Println()
		}
	}
	return nil
}
