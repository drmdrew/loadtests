# FalkorDB / RedisGraph Load Test Driver

This directory contains a Go-based test driver and Docker Compose configurations for reproducing
two distinct failure modes in FalkorDB and its predecessor RedisGraph:

1. **BGSAVE fork hang** — a child process hangs on a `futex` (`pthread_rwlock_t`) inherited from
   the parent at fork time (RedisGraph / FalkorDB ≤ v4.14.x)
2. **BGSAVE fork BUSY** — clients receive `BUSY Redis is busy running a module command` during
   BGSAVE fork preparation in FalkorDB v4.16.x

---

## Background: BGSAVE BUSY in FalkorDB v4.16.x

FalkorDB v4.16.x introduced `RedisModule_Yield(REDISMODULE_YIELD_FLAG_CLIENTS, ...)` calls inside
`_ForkPrepare()` — the `pthread_atfork` prepare handler that runs on the main thread before every
`fork()`. During a BGSAVE, this handler iterates every graph in the keyspace and synchronizes its
GraphBLAS matrices (zero matrix, adjacency, node label, per-label, and per-relation matrices),
calling `Yield` between each sync.

Each `Yield` call runs the Redis event loop briefly with the busy flag set, so any client command
that arrives during a Yield window receives a `BUSY` response. The number of Yield calls per
BGSAVE scales with:

```
total_yields = sum over all graphs of (3 + n_labels + n_relations)
```

With large, schema-rich graphs this generates a sustained stream of brief BUSY windows throughout
the entire fork preparation phase.

The `docker-compose.falkordb-busy-test.yml` compose file attempts to reproduce this by building
graphs shaped similarly to observed production workloads: large node counts, multiple node types
with stacked labels, multiple relation types, and an ongoing expire/repopulate cycle that keeps
matrices dirty ahead of each BGSAVE.

---

## Compose Files

| File | Purpose |
|------|---------|
| `docker-compose.falkordb-busy-test.yml` | FalkorDB v4.16.x BUSY reproduction (primary) |
| `docker-compose.falkordb-test.yml` | FalkorDB v4.14.8 baseline (no BUSY behavior) |
| `docker-compose.bgsave-test.yml` | RedisGraph v2.12.10 BGSAVE fork hang (original) |

---

## FalkorDB BUSY Test

### Start

```bash
cd redisgraph-bgsave

# Default: falkordb/falkordb:v4.16.3
docker compose -p falkordb-busy -f docker-compose.falkordb-busy-test.yml up --build

# Override version
FALKORDB_VERSION=v4.16.7 docker compose -p falkordb-busy -f docker-compose.falkordb-busy-test.yml up --build
```

### Monitor

```bash
# Watch for BUSY errors and Fork preparation time in master logs
docker compose -p falkordb-busy -f docker-compose.falkordb-busy-test.yml logs -f falkordb-master \
  | grep -E "BUSY|Fork preparation|bgsave|fork"

# Watch test driver output (BUSY error counter)
docker compose -p falkordb-busy -f docker-compose.falkordb-busy-test.yml logs -f falkordb-testdriver
```

### What to look for

In `falkordb-master` logs:
- `Fork preparation time: X.XXXXXX sec` — logged for every fork (BGSAVE and GC). BGSAVE preps
  will have larger values than GC preps (which are typically sub-millisecond).
- `Background saving started` — marks each BGSAVE; BUSY windows occur during fork prep preceding this line.

In `falkordb-testdriver` logs:
- `busyErrors=N` in the periodic status line — cumulative count of `BUSY` responses received by
  the query workers.

### Stop

```bash
docker compose -p falkordb-busy -f docker-compose.falkordb-busy-test.yml down -v
```

---

## FalkorDB Baseline Test (v4.14.x)

Uses `docker-compose.falkordb-test.yml` with FalkorDB v4.14.8 (no Yield calls in `_ForkPrepare`,
no BUSY behavior expected).

```bash
docker compose -p falkordb-test -f docker-compose.falkordb-test.yml up --build
docker compose -p falkordb-test -f docker-compose.falkordb-test.yml down -v
```

---

## Configuration

All options are set via environment variables on the `falkordb-testdriver` / `redisgraph-testdriver`
service. The compose files show the values used for each test scenario.

### Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_MASTER` | `redisgraph-master:6379` | Master address |
| `REDIS_REPLICA` | `redisgraph-replica:6379` | Replica address |
| `REDIS_REPLICA2` | _(empty)_ | Optional second replica |
| `QUERY_REPLICAS` | _(empty)_ | Comma-separated addresses to send read queries to (empty = master only) |
| `EXIT_ON_MASTER_LOSS` | `true` | Exit if master becomes unreachable |

### Graph shape

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_GRAPHS` | `100` | Number of graphs to create |
| `TARGET_NODES_PER_GRAPH` | `200` | Node count per graph |
| `NUM_NODE_TYPES` | `10` | Number of distinct node type labels cycled during population |
| `NUM_LABELS_PER_TYPE` | `10` | Additional stacked labels per node type (increases label matrix count) |
| `SIMPLE_SEED_ONLY` | `false` | If `true`, create one node per graph only |

### Workers

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_UPDATE_WORKERS` | `1` | Concurrent node update workers |
| `UPDATE_NODE_COUNT` | `0` | Nodes updated per operation (0 = all) |
| `UPDATE_INTERVAL` | `1s` | Interval between updates per worker |
| `NUM_DYNAMIC_GRAPH_WORKERS` | `1` | Workers that delete and recreate graphs |
| `DYNAMIC_GRAPH_INTERVAL` | `1s` | Interval between dynamic graph operations |
| `DYNAMIC_GRAPH_NODE_COUNT` | `100` | Nodes deleted/recreated per dynamic graph operation |
| `NUM_QUERY_WORKERS` | `1` | Concurrent read query workers |
| `QUERY_INTERVAL` | `2s` | Interval between queries per worker |
| `NUM_GC_GARBAGE_WORKERS` | `1` | Workers that create and immediately delete nodes (GC pressure) |
| `GC_GARBAGE_INTERVAL` | `2s` | Interval between GC garbage operations |
| `GC_GARBAGE_NODE_COUNT` | `50` | Nodes created/deleted per GC garbage operation |

### Expire/repopulate (production-shaped workload)

These workers model a pattern where nodes are marked as logically expired, then bulk-deleted
ahead of BGSAVE, keeping the graph matrices dirty at fork time.

| Variable | Default | Description |
|----------|---------|-------------|
| `NUM_EXPIRE_WORKERS` | `1` | Workers that mark nodes as `_expired` |
| `EXPIRE_INTERVAL` | `30s` | How often each expire worker runs |
| `EXPIRE_NODE_COUNT` | `2000` | Nodes marked `_expired` per run |
| `NUM_REPOPULATE_WORKERS` | `=NUM_EXPIRE_WORKERS` | Workers that recreate expired nodes at the same rate |
| `NUM_GC_EXPIRED_WORKERS` | `1` | Workers that bulk-delete `_expired` nodes before BGSAVE |
| `GC_PRE_BGSAVE_OFFSET` | `10s` | How long before BGSAVE the GC-expired worker fires |
| `NUM_EXPIRE_REL_WORKERS` | `1` | Workers that expire stale relationships (full scan, no LIMIT) |
| `EXPIRE_REL_INTERVAL` | `60s` | How often each expire-relations worker runs |
| `STALE_REL_THRESHOLD_SEC` | `30` | Relationships older than this (seconds) are marked `_expired` |

### Timing

| Variable | Default | Description |
|----------|---------|-------------|
| `BGSAVE_INTERVAL` | `5s` | How often the driver triggers `BGSAVE` on master |
| `JITTER_MAX_MS` | `100` | Maximum random jitter added to desynchronize workers |

---

## BGSAVE Fork Hang Test (RedisGraph)

The original test targets a `pthread_rwlock_t` deadlock in RedisGraph where the BGSAVE child
inherits a locked mutex from the parent.

```bash
docker compose -p bgsave-test -f docker-compose.bgsave-test.yml up -d

# Check for stuck BGSAVE child processes
docker exec redisgraph-master ps aux

docker compose -p bgsave-test -f docker-compose.bgsave-test.yml down -v
```

The hang manifests as a BGSAVE child visible in `ps` that never exits, with future `BGSAVE`
requests blocked indefinitely.
