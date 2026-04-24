package linearizability

import "testing"

func TestSequentialIsLinearizable(t *testing.T) {
	h := []Op{
		{ClientID: 1, Kind: "Put", Key: "k", Value: "a", Invoke: 1, Return: 2},
		{ClientID: 1, Kind: "Append", Key: "k", Value: "b", Invoke: 3, Return: 4},
		{ClientID: 1, Kind: "Get", Key: "k", Result: "ab", Invoke: 5, Return: 6},
	}
	if !CheckSingleKey(h, "k") {
		t.Fatal("expected linearizable")
	}
}

func TestImpossibleHistoryRejected(t *testing.T) {
	h := []Op{
		{ClientID: 1, Kind: "Put", Key: "k", Value: "a", Invoke: 1, Return: 2},
		{ClientID: 1, Kind: "Get", Key: "k", Result: "z", Invoke: 3, Return: 4},
	}
	if CheckSingleKey(h, "k") {
		t.Fatal("expected non-linearizable")
	}
}

func TestConcurrentOverlap(t *testing.T) {
	h := []Op{
		{ClientID: 1, Kind: "Put", Key: "k", Value: "a", Invoke: 1, Return: 5},
		{ClientID: 2, Kind: "Get", Key: "k", Result: "a", Invoke: 2, Return: 6},
	}
	if !CheckSingleKey(h, "k") {
		t.Fatal("overlap should be linearizable")
	}
}
