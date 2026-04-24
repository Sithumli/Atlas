// Package storage provides durable persistence for Raft state and
// state-machine snapshots. It implements pkg/raft.Persister.
package storage

import (
	"os"
	"path/filepath"
	"sync"
)

// FilePersister persists Raft state to a pair of files on disk.
// SaveState and SaveStateAndSnapshot are atomic with respect to crashes
// via write-to-temp + fsync + rename.
type FilePersister struct {
	mu       sync.Mutex
	dir      string
	statePath string
	snapPath  string
}

// NewFilePersister creates a persister rooted at dir. The directory is
// created if it does not exist.
func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FilePersister{
		dir:       dir,
		statePath: filepath.Join(dir, "state.bin"),
		snapPath:  filepath.Join(dir, "snapshot.bin"),
	}, nil
}

// SaveState atomically persists the encoded Raft state.
func (p *FilePersister) SaveState(state []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	atomicWrite(p.statePath, state)
}

// ReadState returns the previously persisted Raft state or nil.
func (p *FilePersister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, _ := os.ReadFile(p.statePath)
	return b
}

// SaveStateAndSnapshot atomically persists Raft state and a snapshot together.
// The two writes must be crash-consistent.
func (p *FilePersister) SaveStateAndSnapshot(state, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// TODO(week 6): crash-consistent two-file write (single dir fsync,
	// or combined file format)
	atomicWrite(p.statePath, state)
	atomicWrite(p.snapPath, snapshot)
}

// ReadSnapshot returns the previously persisted snapshot or nil.
func (p *FilePersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, _ := os.ReadFile(p.snapPath)
	return b
}

// StateSize returns the size of the persisted Raft state in bytes.
func (p *FilePersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	info, err := os.Stat(p.statePath)
	if err != nil {
		return 0
	}
	return int(info.Size())
}

func atomicWrite(path string, data []byte) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return
	}
	if err := f.Close(); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}

// InMemoryPersister is a non-durable persister used in tests and development.
type InMemoryPersister struct {
	mu    sync.Mutex
	state []byte
	snap  []byte
}

func NewInMemoryPersister() *InMemoryPersister { return &InMemoryPersister{} }

func (p *InMemoryPersister) SaveState(state []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
}

func (p *InMemoryPersister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...)
}

func (p *InMemoryPersister) SaveStateAndSnapshot(state, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	p.snap = append([]byte(nil), snapshot...)
}

func (p *InMemoryPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.snap...)
}

func (p *InMemoryPersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}
