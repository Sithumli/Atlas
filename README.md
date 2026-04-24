# Atlas

A linearizable, Raft-backed, sharded key-value store written in Go.

Atlas replicates data across groups of nodes that elect leaders via the Raft
consensus algorithm, partitions the keyspace across multiple groups for
horizontal scale, and supports live shard migration without dropping client
requests. Inspired by MIT 6.824, Google Spanner's sharding model, and the Raft
paper (Ongaro & Ousterhout, 2014).

See [`docs/Atlas.tex`](docs/Atlas.tex) for the full technical specification.

## Layout

```
atlas/
├── cmd/                    # one binary per executable
│   ├── atlas-ctrler/       # shard controller server
│   ├── atlas-kv/           # sharded KV server
│   └── atlas-cli/          # admin CLI
├── pkg/
│   ├── raft/               # Raft consensus library
│   ├── kvraft/             # single-group linearizable KV on Raft
│   ├── shardctrler/        # configuration service
│   ├── shardkv/            # sharded KV server
│   ├── client/             # client library (routing, retries)
│   └── rpc/                # RPC abstraction + fault injection
├── internal/
│   ├── storage/            # persistence (BoltDB/Pebble wrappers)
│   └── metrics/            # Prometheus metric definitions
├── test/
│   ├── chaos/              # end-to-end chaos matrix
│   └── linearizability/    # Porcupine-driven tests
├── deploy/
│   ├── docker-compose.yml  # local multi-node cluster
│   └── k8s/                # StatefulSet manifests
├── proto/                  # protobuf wire definitions
└── docs/
    └── Atlas.tex           # technical specification
```

## Quick start

```bash
# build everything
make build

# run unit tests with the race detector
make test

# spin up a local 9-node cluster
make up
```

## Status

Early work-in-progress. See [milestones](docs/Atlas.tex) in the spec for the
week-by-week plan.

## License

See [LICENSE](LICENSE).
