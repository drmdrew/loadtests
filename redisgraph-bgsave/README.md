# BGSAVE Hang Test Driver

This test driver is designed to reproduce the BGSAVE hang condition observed in RedisGraph where a forked child process hangs waiting on a futex (pthread_rwlock_t).

## Setup

The test uses Docker Compose to orchestrate:
- `redisgraph-master`: RedisGraph v2.12.10 instance (internal network only, no exposed ports)
- `redisgraph-replica`: RedisGraph v2.12.10 replica (internal network only, no exposed ports)
- `redisgraph-testdriver`: Go-based test driver that populates graphs and runs stress tests

**Note**: This setup uses an isolated Docker network and does not expose ports by default to avoid conflicts with other Redis instances. Services communicate via internal Docker networking.

If you need external access to Redis (e.g., for debugging), uncomment the `ports` sections in `docker-compose.bgsave-test.yml` and use non-conflicting ports (e.g., 16379, 16380).

## Usage

### Start the test stack

```bash
cd loadtest/bgsave
# Use custom project name for isolation
docker-compose -p bgsave-test -f docker-compose.bgsave-test.yml up -d
```

Or without the project name (uses directory name):
```bash
docker-compose -f docker-compose.bgsave-test.yml up -d
```

### Monitor logs

```bash
# View all logs
docker-compose -p bgsave-test -f docker-compose.bgsave-test.yml logs -f

# View only test driver logs
docker-compose -p bgsave-test -f docker-compose.bgsave-test.yml logs -f redisgraph-testdriver

# View master logs
docker-compose -p bgsave-test -f docker-compose.bgsave-test.yml logs -f redisgraph-master
```

### Check for hanging BGSAVE processes

```bash
# Check master process list
docker exec redisgraph-master ps aux

# Check replica process list
docker exec redisgraph-replica ps aux

# Look for stuck BGSAVE child processes (should show futex_wait_queue in WCHAN)
```

### Stop the test stack

```bash
docker-compose -p bgsave-test -f docker-compose.bgsave-test.yml down -v
```

## Configuration

Environment variables (set in docker-compose or override):

- `REDIS_MASTER`: Master Redis address (default: `redisgraph-master:6379`)
- `REDIS_REPLICA`: Replica Redis address (default: `redisgraph-replica:6379`)
- `NUM_GRAPHS`: Number of graphs to create (default: `100`)
- `TARGET_NODES_PER_GRAPH`: Number of nodes to create per graph when using full population mode (default: `200`)
- `NUM_UPDATE_WORKERS`: Number of concurrent update workers (default: `1`). More workers increase the chance of the RW lock being held during fork.
- `NUM_DYNAMIC_GRAPH_WORKERS`: Number of dynamic graph create/delete workers (default: `1`). These workers delete and recreate graphs to trigger `Globals_RemoveGraphByName` and `Globals_AddGraph` which acquire the global write lock.
- `NUM_QUERY_WORKERS`: Number of concurrent query workers (default: `1`). These workers perform graph-walking queries that traverse relationships, randomly choosing between master and allowed replicas.
- `QUERY_REPLICAS`: Comma-separated list of replica addresses to query (default: empty, meaning only master). Examples:
  - Empty or unset: Only master receives queries (replicas only get replication traffic)
  - `redisgraph-replica:6379`: Only replica-1 receives queries
  - `redisgraph-replica:6379,redisgraph-replica2:6379`: Both replicas receive queries
- `JITTER_MAX_MS`: Maximum random jitter in milliseconds (0-1000) added to desynchronize workers and BGSAVE triggers (default: `100`). Higher values increase timing variability.
- `UPDATE_INTERVAL`: Interval between graph updates per worker (default: `1s`)
- `DYNAMIC_GRAPH_INTERVAL`: Interval between dynamic graph create/delete operations per worker (default: `1s`)
- `QUERY_INTERVAL`: Interval between graph-walking queries per worker (default: `2s`)
- `BGSAVE_INTERVAL`: Interval between BGSAVE triggers (default: `5s`)
- `SIMPLE_SEED_ONLY`: If `true`, only create one node per graph (for testing node creation). If `false`, use full population with `TARGET_NODES_PER_GRAPH` nodes per graph (default: `false`)

## How It Works

1. **Population Phase**: On startup, the test driver checks for graphs `graph-0` through `graph-99`. Missing or under-populated graphs are populated using a quadratic relationship pattern (`MATCH (a),(b) CREATE (a)-[:REL]->(b)`) to create moderate graph sizes.

2. **Stress Phase**: Once graphs are populated, multiple concurrent workers run:
   - **Update Worker**: Randomly selects a graph and updates all nodes with a timestamp attribute
   - **Dynamic Graph Worker**: Randomly deletes and recreates graphs (using `GRAPH.LIST` to find existing graphs), triggering `Globals_RemoveGraphByName` and `Globals_AddGraph` which acquire the global write lock
   - **Query Worker**: Performs graph-walking queries that traverse relationships, randomly choosing between master and replica to stress both instances
   - **BGSAVE Worker**: Triggers `BGSAVE` on both master and replica at regular intervals

The goal is to create a timing window where a worker thread holds the `_globals.lock` rwlock when Redis forks for BGSAVE, causing the child process to inherit a locked mutex and hang.

## Expected Behavior

- Normal operation: BGSAVE completes successfully, new BGSAVE requests are accepted
- Hang condition: BGSAVE child process appears in `ps` output but doesn't complete, future BGSAVE requests are blocked

