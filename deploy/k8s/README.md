# Kubernetes deployment

Production-shaped manifests go here (stretch goal, week 16+).

Sketch:

- `namespace.yaml` — `atlas` namespace
- `ctrler-statefulset.yaml` — 3-replica StatefulSet with PVC, headless Service
- `shardkv-statefulset.yaml` — one StatefulSet per replica group (parameterised by GID)
- `pdb.yaml` — PodDisruptionBudget ensuring a majority remains live during rolling restarts
- `servicemonitor.yaml` — Prometheus Operator scrape config
