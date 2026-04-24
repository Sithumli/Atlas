// Package metrics holds the Prometheus metric definitions emitted by the
// Raft library, the KV state machines, and the shard controller.
//
// This file intentionally declares only metric names and descriptions.
// The actual Prometheus registration wiring is added when the server
// binaries gain their HTTP /metrics endpoint (week 15).
package metrics

// Metric names. Keep them stable — dashboards and alerts depend on them.
const (
	RaftTermTotal            = "atlas_raft_term_total"
	RaftLeaderChangesTotal   = "atlas_raft_leader_changes_total"
	RaftCommitLatencySeconds = "atlas_raft_commit_latency_seconds"
	RaftLogSizeBytes         = "atlas_raft_log_size_bytes"
	RaftSnapshotBytesTotal   = "atlas_raft_snapshot_bytes_total"

	KVOpsTotal               = "atlas_kv_ops_total"
	KVOpLatencySeconds       = "atlas_kv_op_latency_seconds"

	ShardMigrationDuration   = "atlas_shard_migration_duration_seconds"
	ShardMigrationTotal      = "atlas_shard_migration_total"

	ConfigNum                = "atlas_config_num"
)
