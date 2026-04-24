// Package chaos holds the end-to-end chaos test matrix for Atlas.
//
// Tests run an in-process cluster against the deterministic rpc.Network
// fault-injection transport, driving the cluster through combinations of:
//
//   - reliable / lossy / partitioned networks
//   - quiescent / frequent configuration churn
//   - no / periodic / adversarial node restarts
//
// Client histories are recorded and checked for linearizability
// (see test/linearizability).
//
// Tests will be populated during weeks 15--16 per docs/Atlas.tex.
package chaos
