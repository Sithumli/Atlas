// Package client is the user-facing Atlas client library.
package client

import (
	"errors"

	"github.com/Sithumli/atlas/pkg/raft"
	"github.com/Sithumli/atlas/pkg/shardkv"
)

// Clerk is a handle used by application code to talk to an Atlas cluster.
type Clerk struct {
	inner *shardkv.Clerk
}

// New creates a new client.
func New(ctrlers []raft.Peer, makeEnd func(addr string) raft.Peer) *Clerk {
	return &Clerk{inner: shardkv.MakeClerk(ctrlers, makeEnd)}
}

// Errors returned to the caller.
var (
	ErrTimeout = errors.New("atlas: request timed out")
	ErrClosed  = errors.New("atlas: client is closed")
)

// Get returns the value for key.
func (c *Clerk) Get(key string) (string, error) {
	return c.inner.Get(key), nil
}

// Put writes value at key.
func (c *Clerk) Put(key, value string) error {
	c.inner.Put(key, value)
	return nil
}

// Append atomically appends value to the existing string at key.
func (c *Clerk) Append(key, value string) error {
	c.inner.Append(key, value)
	return nil
}
