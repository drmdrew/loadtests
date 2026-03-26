# loadtests

Load test harnesses for reproducing failure modes in FalkorDB and RedisGraph.

## Contents

- [redisgraph-bgsave/](redisgraph-bgsave/) — Test driver for BGSAVE-related failures:
  - FalkorDB v4.16.x `BUSY` signal during BGSAVE fork preparation
  - RedisGraph BGSAVE fork hang (`pthread_rwlock_t` deadlock in child)
