package main

import (
	"fmt"
	"log"
	"os"

	"github.com/gtfierro/pundat/client"
	bw "gopkg.in/immesys/bw2bind.v5"
)

func main() {
	bwclient := bw.ConnectOrExit("")
	bwclient.OverrideAutoChainTo(true)
	vk := bwclient.SetEntityFromEnvironOrExit()

	pc, err := client.NewPundatClient(bwclient, vk, "ucberkeley")
	if err != nil {
		log.Fatal(err)
	}
	md, ts, ch, err := pc.Query(os.Args[1])

	fmt.Println("md", md)
	fmt.Println("ts", ts)
	fmt.Println("ch", ch)
	fmt.Println("err", err)
}
