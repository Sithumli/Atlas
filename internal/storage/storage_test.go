package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	state := []byte("state-v1")
	snap := []byte("snap-v1")
	p.SaveStateAndSnapshot(state, snap)
	if !bytes.Equal(p.ReadState(), state) {
		t.Fatalf("state read != written")
	}
	if !bytes.Equal(p.ReadSnapshot(), snap) {
		t.Fatalf("snapshot read != written")
	}
	if p.StateSize() != len(state) {
		t.Fatalf("StateSize=%d want %d", p.StateSize(), len(state))
	}
	if _, err := os.Stat(filepath.Join(dir, "state.bin")); err != nil {
		t.Fatalf("expected state.bin: %v", err)
	}
}

func TestInMemoryPersisterRoundTrip(t *testing.T) {
	p := NewInMemoryPersister()
	state := []byte("alpha")
	snap := []byte("beta")
	p.SaveStateAndSnapshot(state, snap)
	if !bytes.Equal(p.ReadState(), state) {
		t.Fatal("state mismatch")
	}
	if !bytes.Equal(p.ReadSnapshot(), snap) {
		t.Fatal("snap mismatch")
	}
	p.SaveState([]byte("gamma"))
	if !bytes.Equal(p.ReadState(), []byte("gamma")) {
		t.Fatal("SaveState did not overwrite")
	}
	if !bytes.Equal(p.ReadSnapshot(), snap) {
		t.Fatal("SaveState should not affect snapshot")
	}
}

func TestFilePersisterAtomicCreate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "deeper")
	p, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	p.SaveState([]byte("hello"))
	if got := string(p.ReadState()); got != "hello" {
		t.Fatalf("got %q", got)
	}
}
