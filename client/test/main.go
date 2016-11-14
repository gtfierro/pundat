package main

import (
	"fmt"
	"github.com/gtfierro/pundat/client"
	bw "gopkg.in/immesys/bw2bind.v5"
	"os"
)

func main() {
	bwclient := bw.ConnectOrExit("")
	bwclient.OverrideAutoChainTo(true)
	vk := bwclient.SetEntityFromEnvironOrExit()

	pc := client.NewPundatClient(bwclient, vk, "ucberkeley")
	ts, md, ch, err := pc.Query(os.Args[1])

	fmt.Println("ts", ts)
	fmt.Println("md", md)
	fmt.Println("ch", ch)
	fmt.Println("err", err)
}
