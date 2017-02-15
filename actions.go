package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gtfierro/pundat/archiver"
	"github.com/gtfierro/pundat/client"
	"github.com/gtfierro/pundat/dots"

	"github.com/immesys/bw2/objects"
	"github.com/immesys/bw2/util"
	bw2 "github.com/immesys/bw2bind"
	"github.com/mgutz/ansi"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
	"gopkg.in/readline.v1"
)

func resolveKey(client *bw2.BW2Client, key string) (string, error) {
	if _, err := os.Stat(key); err != nil && !os.IsNotExist(err) {
		return "", errors.Wrap(err, "Could not check key file")
	} else if err == nil {
		// have a file and load it!
		contents, err := ioutil.ReadFile(key)
		if err != nil {
			return "", errors.Wrap(err, "Could not read file")
		}
		entity, err := objects.NewEntity(int(contents[0]), contents[1:])
		if err != nil {
			return "", errors.Wrap(err, "Could not decode entity from file")
		}
		ent, ok := entity.(*objects.Entity)
		if !ok {
			return "", errors.New(fmt.Sprintf("File was not an entity: %s", key))
		}
		key_vk := objects.FmtKey(ent.GetVK())
		return key_vk, nil
	} else {
		// resolve key from registry
		a, b, err := client.ResolveRegistry(key)
		if err != nil {
			return "", errors.Wrapf(err, "Could not resolve key %s", key)
		}
		if b != bw2.StateValid {
			return "", errors.New(fmt.Sprintf("Key was not valid: %s", key))
		}
		ent, ok := a.(*objects.Entity)
		if !ok {
			return "", errors.New(fmt.Sprintf("Key was not an entity: %s", key))
		}
		key_vk := objects.FmtKey(ent.GetVK())
		return key_vk, nil
	}
}

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

func scanWithClient(client *bw2.BW2Client, uri string) ([]string, []time.Time, error) {
	var found []string
	var times []time.Time

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

func scan(uri, entity, agent string) ([]string, []time.Time, error) {
	client := bw2.ConnectOrExit(agent)
	client.SetEntityFileOrExit(entity)
	client.OverrideAutoChainTo(true)

	return scanWithClient(client, uri)
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
	bw2.SilenceLog()
	bwclient := bw2.ConnectOrExit(c.String("agent"))
	bwclient.SetEntityFileOrExit(c.String("entity"))
	bwclient.OverrideAutoChainTo(true)
	formattime := c.Bool("formattime")

	if c.NArg() == 0 {
		return errors.New("Need to specify a namespace or URI prefix of an archiver (can use 'pundat scan' to help)")
	}
	archiverURI := c.Args().Get(0)

	pc, err := client.NewPundatClientFromConfig(c.String("entity"), c.String("agent"), archiverURI)
	if err != nil {
		return err
	}

	var foundArchiver = false
	res, err := bwclient.Query(&bw2.QueryParams{
		URI: archiverURI + "/s.giles/!meta/lastalive",
	})
	if err != nil {
		log.Error(err)
	} else {
		for msg := range res {
			foundArchiver = true
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

	if !foundArchiver {
		log.Fatalf("No archiver found at %s", archiverURI)
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
			if formattime {
				fmt.Println(ts.DumpWithFormattedTime())
			} else {
				fmt.Println(ts.Dump())
			}
		}
		if !ch.IsEmpty() {
			fmt.Println(ch.Dump())
		}
	}
	return nil
}

func doScan(c *cli.Context) error {
	bw2.SilenceLog()
	if c.NArg() == 0 {
		return errors.New("Need to specify a namespace or URI prefix to scan")
	}

	archivers, times, err := scan(c.Args().Get(0), c.String("entity"), c.String("agent"))
	if err != nil {
		log.Fatal(err)
	}

	if len(archivers) == 0 {
		log.Fatalf("No archiver found at %s", c.Args().Get(0))
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

func doCheck(c *cli.Context) error {
	bw2.SilenceLog()
	key := c.String("key")
	if key == "" {
		log.Fatal(errors.New("Need to specify key"))
	}
	uri := c.String("uri")
	if uri == "" {
		log.Fatal(errors.New("Need to specify uri"))
	}
	entity := c.String("entity")
	agent := c.String("agent")
	// connect
	bwclient := bw2.ConnectOrExit(agent)
	bwclient.SetEntityFileOrExit(entity)
	bwclient.OverrideAutoChainTo(true)
	_, _, err := checkAccess(bwclient, key, uri)
	if err != nil {
		log.Error("Likely that key does not have access to archiver")
		log.Fatal(err)
	}
	return nil
}

func doGrant(c *cli.Context) error {
	bw2.SilenceLog()
	key := c.String("key")
	if key == "" {
		log.Fatal(errors.New("Need to specify key"))
	}
	uri := c.String("uri")
	if uri == "" {
		log.Fatal(errors.New("Need to specify uri"))
	}
	entity := c.String("entity")
	bankroll := c.String("bankroll")
	agent := c.String("agent")
	if c.String("expiry") == "" {
		log.Fatal(errors.New("Need to specify expiry"))
	}
	expiry, err := util.ParseDuration(c.String("expiry"))
	if err != nil {
		log.Fatal(errors.Wrap(err, "Could not parse expiry"))
	}
	// connect
	bwclient := bw2.ConnectOrExit(agent)
	bwclient.SetEntityFileOrExit(entity)
	bwclient.OverrideAutoChainTo(true)

	uris, access, err := checkAccess(bwclient, key, uri)
	if err != nil {
		log.Error(err, "(This is probably OK)")
	}

	key_vk, err := resolveKey(bwclient, key)
	if err != nil {
		log.Fatal(err)
	}

	datmoney := bw2.ConnectOrExit(agent)
	datmoney.SetEntityFileOrExit(bankroll)
	datmoney.OverrideAutoChainTo(true)

	var dotsToPublish [][]byte
	var hashToPublish []string
	var urisToPublish []string
	successcolor := ansi.ColorFunc("green")

	// scan URI
	scanAccess := access[0]
	if !scanAccess {
		// grant dot
		scanURI := uris[0]
		params := &bw2.CreateDOTParams{
			To:                key_vk,
			TTL:               0,
			Comment:           fmt.Sprintf("Access to archiver on URI %s", uri),
			URI:               scanURI,
			ExpiryDelta:       expiry,
			AccessPermissions: "C*",
		}
		hash, blob, err := bwclient.CreateDOT(params)
		if err != nil {
			log.Fatal(errors.Wrap(err, fmt.Sprintf("Could not grant DOT to %s on %s with permissions C*", key_vk, scanURI)))
		}
		log.Info("Granting DOT", hash)
		dotsToPublish = append(dotsToPublish, blob)
		hashToPublish = append(hashToPublish, hash)
		urisToPublish = append(urisToPublish, scanURI)
	}

	// query URI
	queryAccess := access[1]
	if !queryAccess {
		// grant dot
		queryURI := uris[1]
		params := &bw2.CreateDOTParams{
			To:                key_vk,
			TTL:               0,
			Comment:           fmt.Sprintf("Access to archiver on URI %s", uri),
			URI:               queryURI,
			ExpiryDelta:       expiry,
			AccessPermissions: "P",
		}
		hash, blob, err := bwclient.CreateDOT(params)
		if err != nil {
			log.Fatal(errors.Wrap(err, fmt.Sprintf("Could not grant DOT to %s on %s with permissions P", key_vk, queryURI)))
		}
		log.Info("Granting DOT", hash)
		dotsToPublish = append(dotsToPublish, blob)
		hashToPublish = append(hashToPublish, hash)
		urisToPublish = append(urisToPublish, queryURI)
	}

	// response URI
	responseAccess := access[2]
	if !responseAccess {
		// grant dot
		responseURI := uris[2]
		params := &bw2.CreateDOTParams{
			To:                key_vk,
			TTL:               0,
			Comment:           fmt.Sprintf("Access to archiver on URI %s", uri),
			URI:               responseURI,
			ExpiryDelta:       expiry,
			AccessPermissions: "C",
		}
		hash, blob, err := bwclient.CreateDOT(params)
		if err != nil {
			log.Fatal(errors.Wrap(err, fmt.Sprintf("Could not grant DOT to %s on %s with permissions C", key_vk, responseURI)))
		}
		log.Info("Granting DOT", hash)
		dotsToPublish = append(dotsToPublish, blob)
		hashToPublish = append(hashToPublish, hash)
		urisToPublish = append(urisToPublish, responseURI)
	}

	var wg sync.WaitGroup
	wg.Add(len(dotsToPublish))
	quit := make(chan bool)
	var once sync.Once

	for idx, blob := range dotsToPublish {
		blob := blob
		hash := hashToPublish[idx]
		uri := urisToPublish[idx]
		go func(blob []byte, hash string) {
			log.Info("Publishing DOT", hash)
			defer wg.Done()
			a, err := datmoney.PublishDOT(blob)
			once.Do(func() { quit <- true }) // quit the progress bar
			if err != nil {
				log.Error(errors.Wrap(err, fmt.Sprintf("Could not publish DOT with hash %s (%s)", hash, uri)))
			} else {
				log.Info(successcolor(fmt.Sprintf("Successfully published DOT %s (%s)", a, uri)))
			}
		}(blob, hash)
	}
	// "status bar"
	go func() {
		tick := time.Tick(2 * time.Second)
		for {
			select {
			case <-quit:
				return
			case <-tick:
				fmt.Print(".")
			}
		}
	}()
	wg.Wait()

	return nil
}

func checkAccess(bwclient *bw2.BW2Client, key, uri string) (uris []string, hasPermission []bool, err error) {

	// first check if the archiver is alive:

	// grab the list of archivers off of that URI
	archivers, alives, err := scanWithClient(bwclient, uri)
	if err != nil {
		return
	}

	successcolor := ansi.ColorFunc("green")
	foundcolor := ansi.ColorFunc("blue+h")
	badcolor := ansi.ColorFunc("yellow+b")
	foundArchiver := false
	for idx, archiver := range archivers {
		if archiver == uri {
			ago := time.Since(alives[idx])
			if ago.Minutes() < time.Duration(5*time.Minute).Minutes() {
				fmt.Println(foundcolor(fmt.Sprintf("Found archiver at: %s (alive %v ago)", uri, ago)))
				foundArchiver = true
				break
			} else {
				fmt.Println(badcolor(fmt.Sprintf("Found old archiver at: %s (alive %v ago)", uri, ago)))
				break
			}
		}
	}
	if !foundArchiver {
		err = errors.New(fmt.Sprintf("No (live) archiver found at %s. Try checking the output of 'pundat scan <namespace>'", uri))
		return
	}

	key_vk, err := resolveKey(bwclient, key)
	if err != nil {
		return
	}

	scanURI := uri + "/*/s.giles/!meta/lastalive"                                              // (C*)
	queryURI := uri + "/s.giles/_/i.archiver/slot/query"                                       // (P)
	responseURI := uri + "/s.giles/_/i.archiver/signal/" + key_vk[:len(key_vk)-1] + ",queries" // (C)
	uris = []string{scanURI, queryURI, responseURI}
	hasPermission = []bool{false, false, false}

	// now check access
	chain, err := bwclient.BuildAnyChain(scanURI, "C*", key_vk)
	if err != nil {
		err = errors.Wrapf(err, "Could not build chain on %s to %s", scanURI, key_vk)
		return
	}
	if chain == nil {
		err = errors.New(fmt.Sprintf("Key %s does not have a chain to find archivers (%s)", key_vk, scanURI))
		return
	} else {
		hasPermission[0] = true
	}

	chain, err = bwclient.BuildAnyChain(queryURI, "P", key_vk)
	if err != nil {
		err = errors.Wrapf(err, "Could not build chain on %s to %s", queryURI, key_vk)
		return
	}
	if chain == nil {
		err = errors.New(fmt.Sprintf("Key %s does not have a chain to publish to the archiver (%s)", key_vk, queryURI))
		return
	} else {
		hasPermission[1] = true
	}

	chain, err = bwclient.BuildAnyChain(responseURI, "C", key_vk)
	if err != nil {
		err = errors.Wrapf(err, "Could not build chain on %s to %s", responseURI, key_vk)
		return
	}
	if chain == nil {
		err = errors.New(fmt.Sprintf("Key %s does not have a chain to consume from the archiver (%s)", key_vk, responseURI))
		return
	} else {
		hasPermission[2] = true
	}

	fmt.Println(successcolor(fmt.Sprintf("Key %s has access to archiver at %s\n", key_vk, uri)))

	return
}

func doRange(c *cli.Context) error {
	bw2.SilenceLog()
	client := bw2.ConnectOrExit(c.String("agent"))
	client.SetEntityFileOrExit(c.String("entity"))
	client.OverrideAutoChainTo(true)
	master := dots.NewDotMaster(client, 10)

	uri := c.String("uri")
	if uri == "" {
		log.Fatal("Need to provide uri")
	}
	key := c.String("key")

	key_vk, err := resolveKey(client, key)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "Could not resolve key %s", key))
	}

	rangeset, err := master.GetValidRanges(c.String("uri"), key_vk)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "Could not check ranges for uri %s against key %s", uri, key_vk))
	}
	fmt.Printf("Key %s has %d valid archival ranges on uri %s:\n", key_vk, len(rangeset.Ranges), uri)
	for _, r := range rangeset.Ranges {
		fmt.Println("  ", r.String())
	}
	return nil
}

func doTime(c *cli.Context) error {
	var supported_formats = []string{"1/2/2006",
		"1-2-2006",
		"1/2/2006 03:04:05 PM MST",
		"1-2-2006 03:04:05 PM MST",

		"1/2/2006 15:04:05 MST",
		"1-2-2006 15:04:05 MST",
		"2006-1-2 15:04:05 MST",

		"1/2/2006 03:04:05 PM",
		"1-2-2006 03:04:05 PM",
		"2006-1-2 03:04:05 PM",

		"1/2/2006 15:04:05",
		"1-2-2006 15:04:05",
		"2006-1-2 15:04:05",

		"1/2/2006 15:04",
		"1-2-2006 15:04",
		"2006-1-2 15:04",
	}
	if c.NArg() == 0 {
		// use current time
		fmt.Println(time.Now().UnixNano())
		return nil
	}
	re := regexp.MustCompile("^([-+]?)([0-9]*)([a-zA-Z]*)$")
	for i := 0; i < c.NArg(); i++ {
		s := c.Args().Get(i)
		results := re.FindAllStringSubmatch(s, -1)
		if len(results) == 0 {
			for _, format := range supported_formats {
				t, err := time.Parse(format, s)
				if err != nil {
					continue
				}
				fmt.Println(t.UnixNano())
				break
			}
		}
	}
	return nil
}
