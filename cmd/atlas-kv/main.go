// Command atlas-kv runs a single sharded KV server replica.
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
		id       = flag.Int("id", 0, "index of this server within -peers")
		peers    = flag.String("peers", "", "comma-separated peer addresses in this replica group")
		gid      = flag.Int("gid", 0, "replica group id this server belongs to")
		ctrlers  = flag.String("ctrlers", "", "comma-separated shard-controller addresses")
		dataDir  = flag.String("data", "./data/atlas-kv", "directory for persistent state")
		listen   = flag.String("listen", ":9300", "address to serve RPC on")
		metrics  = flag.String("metrics", ":9301", "address to serve /metrics on")
	)
	flag.Parse()

	if *peers == "" || *gid == 0 || *ctrlers == "" {
		fmt.Fprintln(os.Stderr, "atlas-kv: -peers, -gid, -ctrlers are required")
		flag.Usage()
		os.Exit(2)
	}

	peerList := strings.Split(*peers, ",")
	ctrlerList := strings.Split(*ctrlers, ",")

	log.Printf("atlas-kv starting: id=%d gid=%d peers=%v ctrlers=%v data=%s listen=%s metrics=%s",
		*id, *gid, peerList, ctrlerList, *dataDir, *listen, *metrics)

	// TODO(week 11): construct persister, raft peers, start shardkv.StartServer,
	// register RPC receivers, serve forever.

	select {}
}
