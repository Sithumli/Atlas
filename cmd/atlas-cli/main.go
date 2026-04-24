// Command atlas-cli is the admin CLI for Atlas. It supports cluster
// introspection (Query) and operator-driven reconfiguration
// (Join, Leave, Move), and simple KV operations (Get, Put, Append).
package main

import (
	"flag"
	"fmt"
	"os"
)

func usage() {
	fmt.Fprintf(os.Stderr, `atlas-cli — admin tool for Atlas

usage:
  atlas-cli -ctrlers <addrs> query [num]
  atlas-cli -ctrlers <addrs> join   <gid> <addr1,addr2,...>
  atlas-cli -ctrlers <addrs> leave  <gid>
  atlas-cli -ctrlers <addrs> move   <shard> <gid>
  atlas-cli -ctrlers <addrs> get    <key>
  atlas-cli -ctrlers <addrs> put    <key> <value>
  atlas-cli -ctrlers <addrs> append <key> <value>
`)
	os.Exit(2)
}

func main() {
	ctrlers := flag.String("ctrlers", "", "comma-separated shard-controller addresses")
	flag.Usage = usage
	flag.Parse()

	if *ctrlers == "" || flag.NArg() == 0 {
		usage()
	}

	cmd := flag.Arg(0)
	switch cmd {
	case "query", "join", "leave", "move", "get", "put", "append":
		// TODO: dial controllers, construct client, dispatch
		fmt.Printf("atlas-cli: %s (not yet implemented)\n", cmd)
	default:
		usage()
	}
}
