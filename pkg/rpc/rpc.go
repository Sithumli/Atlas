// Package rpc provides the RPC abstraction used throughout Atlas.
// It deliberately sits behind an interface so tests can substitute a
// deterministic, fault-injectable in-memory transport (see test/chaos).
package rpc

// ClientEnd is the client handle used to send an RPC to a single remote
// endpoint.
type ClientEnd interface {
	// Call invokes the named method on the remote side. Returns true if
	// the RPC completed (reply populated); false indicates the RPC was
	// dropped, timed out, or the remote was unreachable.
	Call(method string, args any, reply any) bool
}

// Server exposes registered receivers for incoming RPCs.
type Server interface {
	Register(receiver any) error
	Serve() error
	Stop()
}

// Network is the test harness transport: a deterministic in-memory switch
// with controllable drops, delays, partitions, and reorderings.
type Network interface {
	MakeEnd(name string) ClientEnd
	AddServer(name string, srv Server)
	Connect(endName, serverName string)
	Enable(endName string, enabled bool)
	SetReliable(reliable bool)
	SetLongDelays(enabled bool)
	Partition(groups [][]string)
	Cleanup()
}
