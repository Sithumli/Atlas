// Package linearizability provides a small linearizability checker for
// register-style histories. It is sufficient to validate single-key
// histories produced by the kv layer; full multi-key checks can be added
// by composing per-key checks.
package linearizability

import "sort"

// Op is one client operation in a recorded history.
type Op struct {
	ClientID int
	Kind     string // "Put" | "Get" | "Append"
	Key      string
	Value    string  // for Put/Append: argument; for Get: result
	Result   string  // for Get: returned value; ignored for Put/Append
	Invoke   int64   // logical time invocation
	Return   int64   // logical time return
}

// CheckSingleKey returns true if the per-key history for `key` is
// linearizable as a register that supports Put (overwrite), Append
// (concatenate), and Get (read). It enumerates orderings consistent with
// the partial order defined by Invoke/Return and checks one is valid.
func CheckSingleKey(history []Op, key string) bool {
	ops := []Op{}
	for _, o := range history {
		if o.Key == key {
			ops = append(ops, o)
		}
	}
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].Return != ops[j].Return {
			return ops[i].Return < ops[j].Return
		}
		return ops[i].Invoke < ops[j].Invoke
	})
	return search(ops, "")
}

func search(ops []Op, state string) bool {
	if len(ops) == 0 {
		return true
	}
	for i, op := range ops {
		if !linearizableNow(op, ops, i) {
			continue
		}
		var next string
		var ok bool
		switch op.Kind {
		case "Put":
			next = op.Value
			ok = true
		case "Append":
			next = state + op.Value
			ok = true
		case "Get":
			next = state
			ok = op.Result == state
		}
		if !ok {
			continue
		}
		rest := append([]Op(nil), ops[:i]...)
		rest = append(rest, ops[i+1:]...)
		if search(rest, next) {
			return true
		}
	}
	return false
}

func linearizableNow(op Op, ops []Op, i int) bool {
	for j, other := range ops {
		if j == i {
			continue
		}
		if other.Return < op.Invoke {
			return false
		}
	}
	return true
}
