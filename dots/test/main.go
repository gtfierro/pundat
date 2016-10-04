package main

import (
	"fmt"
	"github.com/gtfierro/durandal/dots"
	bw2 "gopkg.in/immesys/bw2bind.v5"
	"log"
	"os"
)

func main() {
	client := bw2.ConnectOrExit("")
	client.SetEntityFromEnvironOrExit()

	dm := dots.NewDotMaster(client)

	vk := os.Args[1]
	uri := os.Args[2]

	ranges, err := dm.GetValidRanges(uri, vk)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ranges)
}
