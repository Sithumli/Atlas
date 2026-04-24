// Command atlas-ctrler runs a single shard-controller replica.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	var (
		id      = flag.Int("id", 0, "index of this server within -peers")
		peers   = flag.String("peers", "", "comma-separated peer addresses in the controller group")
		dataDir = flag.String("data", "./data/atlas-ctrler", "directory for persistent state")
		listen  = flag.String("listen", ":9200", "address to serve RPC on")
		metrics = flag.String("metrics", ":9201", "address to serve /metrics on")
	)
	flag.Parse()

	if *peers == "" {
		fmt.Fprintln(os.Stderr, "atlas-ctrler: -peers is required")
		flag.Usage()
		os.Exit(2)
	}

	peerList := strings.Split(*peers, ",")
	log.Printf("atlas-ctrler starting: id=%d peers=%v data=%s listen=%s metrics=%s",
		*id, peerList, *dataDir, *listen, *metrics)

	// TODO(week 10): construct persister, raft peers, start shardctrler.StartServer,
	// register RPC receivers, serve forever.

	select {}
}
