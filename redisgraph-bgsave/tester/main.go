package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

var busyErrorCount atomic.Int64
var repopulateNodeCounter atomic.Int64

type Config struct {
	MasterAddr             string
	ReplicaAddr            string
	Replica2Addr           string   // Optional second replica address
	QueryReplicas          []string // List of replica addresses to query (empty = only master)
	NumGraphs              int
	TargetNodesPerGraph    int // Number of nodes to create per graph
	NumUpdateWorkers       int // Number of concurrent update workers
	UpdateNodeCount        int // Number of nodes to update per update operation (0 = all nodes)
	NumDynamicGraphWorkers int // Number of dynamic graph create/delete workers
	DynamicGraphNodeCount  int // Number of nodes to delete/recreate per dynamic graph operation
	NumQueryWorkers        int // Number of concurrent query workers
	NumGCGarbageWorkers    int // Number of concurrent GC garbage workers (delete/recreate nodes)
	GCGarbageNodeCount     int // Number of nodes to delete/recreate per GC garbage operation
	JitterMaxMs            int // Maximum jitter in milliseconds (0-1000)
	UpdateInterval         time.Duration
	DynamicGraphInterval   time.Duration // Interval for dynamic graph operations
	QueryInterval          time.Duration // Interval for query operations
	GCGarbageInterval      time.Duration // Interval for GC garbage operations
	BGSAVEInterval         time.Duration
	NumNodeTypes           int  // Number of distinct node types (NodeA, NodeB, ...) cycled during population
	NumLabelsPerType       int  // Number of labels per node type (e.g. :NodeA:NodeA_L0:...:NodeA_L8)
	NumExpireWorkers       int  // Number of workers that mark nodes as _expired
	NumRepopulateWorkers   int  // Number of workers that repopulate nodes at the same rate as expiry
	ExpireInterval         time.Duration // How often each expire worker runs
	ExpireNodeCount        int  // Number of nodes to mark _expired per run
	NumGCExpiredWorkers       int           // Number of workers that bulk-delete _expired nodes before BGSAVE
	GCPreBGSAVEOffset         time.Duration // How long before BGSAVE the GC expired worker fires
	NumExpireRelWorkers       int           // Number of workers that expire stale relationships (no LIMIT)
	ExpireRelInterval         time.Duration // How often each expire-relations worker runs
	StaleRelThresholdSec      int           // Relationships older than this (seconds) are marked _expired
	SimpleSeedOnly         bool // If true, only create one node per graph (for testing)
	ExitOnMasterLoss       bool // If false, don't exit on master connectivity loss (for debugging)
}

func getConfig() Config {
	master := os.Getenv("REDIS_MASTER")
	if master == "" {
		master = "redisgraph-master:6379"
	}

	replica := os.Getenv("REDIS_REPLICA")
	if replica == "" {
		replica = "redisgraph-replica:6379"
	}

	replica2 := os.Getenv("REDIS_REPLICA2")
	// replica2 is optional, defaults to empty string

	// Parse QUERY_REPLICAS as comma-separated list (empty = no replicas, only master)
	queryReplicasStr := os.Getenv("QUERY_REPLICAS")
	var queryReplicas []string
	if queryReplicasStr != "" {
		replicaList := strings.Split(queryReplicasStr, ",")
		for _, addr := range replicaList {
			trimmed := strings.TrimSpace(addr)
			if trimmed != "" {
				queryReplicas = append(queryReplicas, trimmed)
			}
		}
	}
	// If empty, queryReplicas will be empty slice, meaning only master will be queried

	numGraphs := 100
	if n := os.Getenv("NUM_GRAPHS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil {
			numGraphs = parsed
		}
	}

	updateInterval := 1 * time.Second
	if d := os.Getenv("UPDATE_INTERVAL"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			updateInterval = parsed
		}
	}

	bgsaveInterval := 5 * time.Second
	if d := os.Getenv("BGSAVE_INTERVAL"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			bgsaveInterval = parsed
		}
	}

	simpleSeedOnly := false
	if s := os.Getenv("SIMPLE_SEED_ONLY"); s != "" {
		if parsed, err := strconv.ParseBool(s); err == nil {
			simpleSeedOnly = parsed
		}
	}

	targetNodesPerGraph := 200
	if n := os.Getenv("TARGET_NODES_PER_GRAPH"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil {
			targetNodesPerGraph = parsed
		}
	}

	numUpdateWorkers := 1
	if n := os.Getenv("NUM_UPDATE_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			numUpdateWorkers = parsed
		}
	}

	updateNodeCount := 0 // Default: 0 means update all nodes
	if n := os.Getenv("UPDATE_NODE_COUNT"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			updateNodeCount = parsed
		}
	}

	jitterMaxMs := 100 // Default 100ms jitter
	if n := os.Getenv("JITTER_MAX_MS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 && parsed <= 1000 {
			jitterMaxMs = parsed
		}
	}

	numDynamicGraphWorkers := 1
	if n := os.Getenv("NUM_DYNAMIC_GRAPH_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			numDynamicGraphWorkers = parsed
		}
	}

	dynamicGraphInterval := 1 * time.Second
	if d := os.Getenv("DYNAMIC_GRAPH_INTERVAL"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			dynamicGraphInterval = parsed
		}
	}

	dynamicGraphNodeCount := 100 // Default: delete/recreate 100 nodes
	if n := os.Getenv("DYNAMIC_GRAPH_NODE_COUNT"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			dynamicGraphNodeCount = parsed
		}
	}

	numQueryWorkers := 1
	if n := os.Getenv("NUM_QUERY_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			numQueryWorkers = parsed
		}
	}

	queryInterval := 2 * time.Second
	if d := os.Getenv("QUERY_INTERVAL"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			queryInterval = parsed
		}
	}

	numGCGarbageWorkers := 1
	if n := os.Getenv("NUM_GC_GARBAGE_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			numGCGarbageWorkers = parsed
		}
	}

	gcGarbageInterval := 2 * time.Second
	if d := os.Getenv("GC_GARBAGE_INTERVAL"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			gcGarbageInterval = parsed
		}
	}

	gcGarbageNodeCount := 50 // Default: delete/recreate 50 nodes
	if n := os.Getenv("GC_GARBAGE_NODE_COUNT"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			gcGarbageNodeCount = parsed
		}
	}

	numNodeTypes := 10
	if n := os.Getenv("NUM_NODE_TYPES"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			numNodeTypes = parsed
		}
	}

	numLabelsPerType := 10
	if n := os.Getenv("NUM_LABELS_PER_TYPE"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			numLabelsPerType = parsed
		}
	} else if n := os.Getenv("NUM_LABELS"); n != "" {
		// backwards compatibility
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			numLabelsPerType = parsed
		}
	}

	numExpireWorkers := 1
	if n := os.Getenv("NUM_EXPIRE_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			numExpireWorkers = parsed
		}
	}

	numRepopulateWorkers := numExpireWorkers
	if n := os.Getenv("NUM_REPOPULATE_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			numRepopulateWorkers = parsed
		}
	}

	expireInterval := 30 * time.Second
	if d := os.Getenv("EXPIRE_INTERVAL"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			expireInterval = parsed
		}
	}

	expireNodeCount := 2000
	if n := os.Getenv("EXPIRE_NODE_COUNT"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			expireNodeCount = parsed
		}
	}

	numGCExpiredWorkers := 1
	if n := os.Getenv("NUM_GC_EXPIRED_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			numGCExpiredWorkers = parsed
		}
	}

	gcPreBGSAVEOffset := 10 * time.Second
	if d := os.Getenv("GC_PRE_BGSAVE_OFFSET"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			gcPreBGSAVEOffset = parsed
		}
	}

	numExpireRelWorkers := 1
	if n := os.Getenv("NUM_EXPIRE_REL_WORKERS"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			numExpireRelWorkers = parsed
		}
	}

	expireRelInterval := 60 * time.Second
	if d := os.Getenv("EXPIRE_REL_INTERVAL"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			expireRelInterval = parsed
		}
	}

	staleRelThresholdSec := 30
	if n := os.Getenv("STALE_REL_THRESHOLD_SEC"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed >= 0 {
			staleRelThresholdSec = parsed
		}
	}

	exitOnMasterLoss := true // Default to exiting on master loss
	if e := os.Getenv("EXIT_ON_MASTER_LOSS"); e != "" {
		if parsed, err := strconv.ParseBool(e); err == nil {
			exitOnMasterLoss = parsed
		}
	}

	return Config{
		MasterAddr:             master,
		ReplicaAddr:            replica,
		Replica2Addr:           replica2,
		QueryReplicas:          queryReplicas,
		NumGraphs:              numGraphs,
		TargetNodesPerGraph:    targetNodesPerGraph,
		NumUpdateWorkers:       numUpdateWorkers,
		UpdateNodeCount:        updateNodeCount,
		NumDynamicGraphWorkers: numDynamicGraphWorkers,
		DynamicGraphNodeCount:  dynamicGraphNodeCount,
		NumQueryWorkers:        numQueryWorkers,
		NumGCGarbageWorkers:    numGCGarbageWorkers,
		GCGarbageNodeCount:     gcGarbageNodeCount,
		JitterMaxMs:            jitterMaxMs,
		UpdateInterval:         updateInterval,
		DynamicGraphInterval:   dynamicGraphInterval,
		QueryInterval:          queryInterval,
		GCGarbageInterval:      gcGarbageInterval,
		BGSAVEInterval:         bgsaveInterval,
		NumNodeTypes:           numNodeTypes,
		NumLabelsPerType:       numLabelsPerType,
		NumExpireWorkers:       numExpireWorkers,
		NumRepopulateWorkers:   numRepopulateWorkers,
		ExpireInterval:         expireInterval,
		ExpireNodeCount:        expireNodeCount,
		NumGCExpiredWorkers:       numGCExpiredWorkers,
		GCPreBGSAVEOffset:         gcPreBGSAVEOffset,
		NumExpireRelWorkers:       numExpireRelWorkers,
		ExpireRelInterval:         expireRelInterval,
		StaleRelThresholdSec:      staleRelThresholdSec,
		SimpleSeedOnly:            simpleSeedOnly,
		ExitOnMasterLoss:       exitOnMasterLoss,
	}
}

func createRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     "",
		DB:           0,
		DialTimeout:  10 * time.Second, // Timeout for establishing connections
		ReadTimeout:  60 * time.Second, // Timeout for socket reads (increased for slow queries)
		WriteTimeout: 10 * time.Second, // Timeout for socket writes
	})
}

// triggerBGSAVE triggers a BGSAVE on the given Redis client and logs the result
func triggerBGSAVE(ctx context.Context, client *redis.Client, instanceName string) error {
	result, err := client.Do(ctx, "BGSAVE").Result()
	if err != nil {
		log.Printf("Warning: BGSAVE failed on %s: %v", instanceName, err)
		return err
	}
	log.Printf("BGSAVE initiated on %s: %v", instanceName, result)
	return nil
}

// isMasterConnectivityError checks if an error indicates loss of connectivity to the master
func isMasterConnectivityError(err error, masterAddr string) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()

	// Check for DNS lookup failures
	if strings.Contains(errStr, "lookup") && strings.Contains(errStr, "no such host") {
		return true
	}

	// Check for connection refused
	if strings.Contains(errStr, "connection refused") {
		return true
	}

	// Check for network unreachable
	if strings.Contains(errStr, "network is unreachable") {
		return true
	}

	// Check for timeout errors
	if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded") {
		return true
	}

	// Check for dial errors (includes DNS lookup failures)
	if strings.Contains(errStr, "dial tcp") {
		// Extract the hostname from masterAddr to check if error mentions it
		host := masterAddr
		if idx := strings.Index(masterAddr, ":"); idx > 0 {
			host = masterAddr[:idx]
		}
		if strings.Contains(errStr, host) {
			return true
		}
	}

	// Check for network errors using Go's error types
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	return false
}

// isBusyError checks if an error is a Redis BUSY error (module command blocking clients)
func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "BUSY")
}

// handleMasterConnectivityLoss logs the error and optionally exits when master connectivity is lost
func handleMasterConnectivityLoss(err error, masterAddr string, config Config, cancel context.CancelFunc) {
	if config.ExitOnMasterLoss {
		log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", masterAddr, err)
		cancel()
		// Give a brief moment for the log to flush, then exit
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	} else {
		log.Printf("WARNING: Lost contact with master (%s): %v. Continuing (EXIT_ON_MASTER_LOSS=false)...", masterAddr, err)
	}
}

// exitOnMasterConnectivityLoss logs the error and optionally exits when master connectivity is lost
// Used during initialization when cancel context is not available
func exitOnMasterConnectivityLoss(err error, masterAddr string, config Config) {
	if config.ExitOnMasterLoss {
		log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", masterAddr, err)
		time.Sleep(100 * time.Millisecond)
		os.Exit(1)
	} else {
		log.Printf("WARNING: Lost contact with master (%s): %v. Continuing (EXIT_ON_MASTER_LOSS=false)...", masterAddr, err)
	}
}

func graphExists(ctx context.Context, client *redis.Client, graphName string) (bool, error) {
	// Try to get node count - if graph doesn't exist, this will fail
	_, err := getNodeCount(ctx, client, graphName)
	if err != nil {
		// Check if error is because graph doesn't exist
		errStr := err.Error()
		if strings.Contains(errStr, "not found") ||
			strings.Contains(errStr, "does not exist") ||
			strings.Contains(errStr, "Unknown graph") {
			return false, nil
		}
		return false, err
	}
	// Graph exists if we got a count (even if 0)
	return true, nil
}

func getNodeCount(ctx context.Context, client *redis.Client, graphName string) (int, error) {
	// Use GRAPH.QUERY to get node count
	// RedisGraph returns an array: [results, metadata]
	// results is an array: [header, [row1], [row2], ...]
	result, err := client.Do(ctx, "GRAPH.QUERY", graphName, "MATCH (n) RETURN count(n)").Result()
	if err != nil {
		return 0, err
	}

	// Debug logging for first graph only
	if graphName == "graph-0" {
		log.Printf("DEBUG getNodeCount: result type=%T, value=%+v", result, result)
	}

	// Parse the array response
	// Format: [header_row, data_row, metadata]
	// Example: [[count(n)], [[1]], [Cached execution: ...]]
	resultArray, ok := result.([]interface{})
	if !ok || len(resultArray) < 2 {
		if graphName == "graph-0" {
			log.Printf("DEBUG getNodeCount: result is not an array or has < 2 elements, len=%d", len(resultArray))
		}
		return 0, nil
	}

	// resultArray[0] is the header row (e.g., [count(n)])
	// resultArray[1] is the data row (e.g., [[1]])
	// resultArray[2] is metadata

	if graphName == "graph-0" {
		log.Printf("DEBUG getNodeCount: resultArray has %d elements", len(resultArray))
		for i, elem := range resultArray {
			log.Printf("DEBUG getNodeCount: resultArray[%d] type=%T, value=%+v", i, elem, elem)
		}
	}

	// Get the data row (second element)
	// resultArray[1] is [[1]] - an array containing an array
	dataRowWrapper, ok := resultArray[1].([]interface{})
	if !ok || len(dataRowWrapper) < 1 {
		if graphName == "graph-0" {
			log.Printf("DEBUG getNodeCount: resultArray[1] is not an array or is empty")
		}
		return 0, nil
	}

	// The actual data row is the first element of the wrapper
	dataRow, ok := dataRowWrapper[0].([]interface{})
	if !ok || len(dataRow) < 1 {
		if graphName == "graph-0" {
			log.Printf("DEBUG getNodeCount: dataRowWrapper[0] is not an array or is empty")
		}
		return 0, nil
	}

	// The count is in the first column of the data row
	if graphName == "graph-0" {
		log.Printf("DEBUG getNodeCount: dataRow[0] type=%T, value=%+v", dataRow[0], dataRow[0])
	}

	// Try different numeric types
	if countStr, ok := dataRow[0].(string); ok {
		if count, err := strconv.Atoi(countStr); err == nil {
			return count, nil
		}
	}
	if count, ok := dataRow[0].(int64); ok {
		return int(count), nil
	}
	if count, ok := dataRow[0].(int32); ok {
		return int(count), nil
	}
	if count, ok := dataRow[0].(int); ok {
		return count, nil
	}

	return 0, nil
}

func buildNodeTypeLabels(typeIdx int, labelsPerType int) string {
	typeName := fmt.Sprintf("Node%c", rune('A'+typeIdx%26))
	s := ":Node:" + typeName
	for i := 0; i < labelsPerType-1; i++ {
		s += fmt.Sprintf(":%s_L%d", typeName, i)
	}
	return s
}

func populateGraph(ctx context.Context, client *redis.Client, graphName string, targetNodes int, numNodeTypes int, numLabelsPerType int) error {
	log.Printf("Populating graph %s (target: %d nodes, %d types, %d labels/type)", graphName, targetNodes, numNodeTypes, numLabelsPerType)

	// Create an index on :Node(name) to make relationship creation lookups fast.
	client.Do(ctx, "GRAPH.QUERY", graphName, "CREATE INDEX ON :Node(name)") //nolint — ignore error if already exists

	// Create nodes in batches using UNWIND for fast bulk insertion.
	// Each batch gets a single node type; types cycle across batches so all
	// label matrices are represented in the graph.
	const batchSize = 500
	created := 0
	for start := 0; start < targetNodes; start += batchSize {
		end := start + batchSize
		if end > targetNodes {
			end = targetNodes
		}
		count := end - start
		typeLabels := buildNodeTypeLabels((start/batchSize)%numNodeTypes, numLabelsPerType)
		now := time.Now().Unix()
		q := fmt.Sprintf(
			"UNWIND range(0, %d) AS i CREATE (n%s {name: toString(%d + i), _created: %d, _updated: %d})",
			count-1, typeLabels, start, now, now)
		if _, err := client.Do(ctx, "GRAPH.QUERY", graphName, q).Result(); err != nil {
			log.Printf("Warning: batch create failed for %s at offset %d: %v", graphName, start, err)
		}
		created += count
		if created%10000 == 0 || created == targetNodes {
			log.Printf("Populating %s: %d/%d nodes", graphName, created, targetNodes)
		}
	}

	// Create CONNECTS_TO relationships: one per consecutive node pair within each batch.
	// This produces ~targetNodes relationships, giving expireRelationsWorker a full
	// unbounded scan matching production's expirePropertyBasedRelations pattern.
	log.Printf("Creating relationships in %s...", graphName)
	relCreated := 0
	now := time.Now().Unix()
	for start := 0; start < targetNodes-1; start += batchSize {
		end := start + batchSize
		if end > targetNodes-1 {
			end = targetNodes - 1
		}
		count := end - start
		q := fmt.Sprintf(
			"UNWIND range(0, %d) AS i "+
				"WITH toString(%d+i) AS nameA, toString(%d+i+1) AS nameB "+
				"MATCH (a:Node {name: nameA}), (b:Node {name: nameB}) "+
				"CREATE (a)-[:CONNECTS_TO {_updated: %d, _relation_source: null, _expired: null}]->(b)",
			count-1, start, start, now)
		if _, err := client.Do(ctx, "GRAPH.QUERY", graphName, q).Result(); err != nil {
			log.Printf("Warning: batch rel create failed for %s at offset %d: %v", graphName, start, err)
		}
		relCreated += count
		if relCreated%10000 == 0 || relCreated >= targetNodes-1 {
			log.Printf("Creating relationships in %s: %d/%d", graphName, relCreated, targetNodes-1)
		}
	}

	// Verify final node count
	count, err := getNodeCount(ctx, client, graphName)
	if err != nil {
		log.Printf("Warning: could not verify node count for %s: %v", graphName, err)
	} else {
		log.Printf("Graph %s populated with %d nodes (target was %d)", graphName, count, targetNodes)
		if count == 0 {
			log.Printf("ERROR: %s has 0 nodes after population - queries may have failed silently!", graphName)
		}
	}

	return nil
}

// simpleSeedGraph creates a single node in the graph - simple and reliable for testing
func simpleSeedGraph(ctx context.Context, client *redis.Client, graphName string) error {
	now := time.Now().Unix()
	seedQuery := fmt.Sprintf("CREATE (n:Node {name: 'node-00', _created: %d, _updated: %d})", now, now)

	log.Printf("Seeding %s with single node...", graphName)
	result, err := client.Do(ctx, "GRAPH.QUERY", graphName, seedQuery).Result()
	if err != nil {
		return fmt.Errorf("failed to seed graph %s: %w", graphName, err)
	}

	// Log result for first graph only
	if graphName == "graph-0" {
		log.Printf("DEBUG simpleSeed: result type=%T, value=%+v", result, result)
	}

	// Verify node was created
	time.Sleep(100 * time.Millisecond)
	count, err := getNodeCount(ctx, client, graphName)
	if err != nil {
		log.Printf("Warning: Could not verify seed node count for %s: %v", graphName, err)
		return nil // Don't fail, just warn
	}

	if count > 0 {
		log.Printf("✓ %s seeded with %d node(s)", graphName, count)
	} else {
		log.Printf("✗ WARNING: %s shows 0 nodes after seed - node may not have been created!", graphName)
	}

	return nil
}

func ensureGraphsPopulated(ctx context.Context, masterClient *redis.Client, config Config) error {
	if config.SimpleSeedOnly {
		log.Printf("Using simple seed mode - creating one node per graph")
	} else {
		log.Printf("Using full population mode - creating %d nodes per graph", config.TargetNodesPerGraph)
	}

	log.Printf("Checking %d graphs for population...", config.NumGraphs)

	for i := 0; i < config.NumGraphs; i++ {
		graphName := fmt.Sprintf("graph-%d", i)

		exists, err := graphExists(ctx, masterClient, graphName)
		if err != nil {
			if isMasterConnectivityError(err, config.MasterAddr) {
				log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
				exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
			}
			log.Printf("Error checking graph %s: %v", graphName, err)
			continue
		}

		if !exists {
			if config.SimpleSeedOnly {
				// Simple seed: just create one node
				if err := simpleSeedGraph(ctx, masterClient, graphName); err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
						exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
					}
					log.Printf("Failed to seed graph %s: %v", graphName, err)
					return fmt.Errorf("failed to seed graph %s: %w", graphName, err)
				}
			} else {
				// Full population
				log.Printf("Graph %s does not exist, populating...", graphName)
				if err := populateGraph(ctx, masterClient, graphName, config.TargetNodesPerGraph, config.NumNodeTypes, config.NumLabelsPerType); err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
						exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
					}
					log.Printf("Failed to populate graph %s: %v", graphName, err)
					return fmt.Errorf("failed to populate graph %s: %w", graphName, err)
				}
			}
		} else {
			count, err := getNodeCount(ctx, masterClient, graphName)
			if err != nil {
				log.Printf("Warning: could not get node count for %s: %v", graphName, err)
			} else {
				if config.SimpleSeedOnly {
					// In simple seed mode, just check if we have at least one node
					if count == 0 {
						log.Printf("Graph %s exists but has 0 nodes, reseeding...", graphName)
						if err := simpleSeedGraph(ctx, masterClient, graphName); err != nil {
							if isMasterConnectivityError(err, config.MasterAddr) {
								log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
								exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
							}
							log.Printf("Failed to reseed graph %s: %v", graphName, err)
						}
					} else {
						log.Printf("Graph %s already seeded (%d nodes)", graphName, count)
					}
				} else {
					// Full population mode
					threshold := config.TargetNodesPerGraph / 2
					if count < threshold {
						log.Printf("Graph %s has only %d nodes (target: %d), repopulating...", graphName, count, config.TargetNodesPerGraph)
						if err := populateGraph(ctx, masterClient, graphName, config.TargetNodesPerGraph, config.NumNodeTypes, config.NumLabelsPerType); err != nil {
							if isMasterConnectivityError(err, config.MasterAddr) {
								log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
								exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
							}
							log.Printf("Failed to repopulate graph %s: %v", graphName, err)
						}
					} else {
						log.Printf("Graph %s already populated (%d nodes)", graphName, count)
					}
				}
			}
		}
	}

	// Also populate dynamic graphs (skip when NumDynamicGraphWorkers=0 to avoid wasted memory)
	if config.NumDynamicGraphWorkers == 0 {
		log.Printf("Skipping dynamic graph population (NUM_DYNAMIC_GRAPH_WORKERS=0)")
	}
	for i := 0; config.NumDynamicGraphWorkers > 0 && i < config.NumGraphs; i++ {
		graphName := fmt.Sprintf("dynamic-graph-%d", i)

		exists, err := graphExists(ctx, masterClient, graphName)
		if err != nil {
			if isMasterConnectivityError(err, config.MasterAddr) {
				log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
				exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
			}
			log.Printf("Error checking dynamic graph %s: %v", graphName, err)
			continue
		}

		if !exists {
			if config.SimpleSeedOnly {
				// Simple seed: just create one node
				if err := simpleSeedGraph(ctx, masterClient, graphName); err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
						exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
					}
					log.Printf("Failed to seed dynamic graph %s: %v", graphName, err)
					// Don't fail completely, just log and continue
				}
			} else {
				// Full population
				log.Printf("Dynamic graph %s does not exist, populating...", graphName)
				if err := populateGraph(ctx, masterClient, graphName, config.TargetNodesPerGraph, config.NumNodeTypes, config.NumLabelsPerType); err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
						exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
					}
					log.Printf("Failed to populate dynamic graph %s: %v", graphName, err)
					// Don't fail completely, just log and continue
				}
			}
		} else {
			count, err := getNodeCount(ctx, masterClient, graphName)
			if err != nil {
				if isMasterConnectivityError(err, config.MasterAddr) {
					log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
					exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
				}
				log.Printf("Warning: could not get node count for %s: %v", graphName, err)
			} else {
				if config.SimpleSeedOnly {
					// In simple seed mode, just check if we have at least one node
					if count == 0 {
						log.Printf("Dynamic graph %s exists but has 0 nodes, reseeding...", graphName)
						if err := simpleSeedGraph(ctx, masterClient, graphName); err != nil {
							if isMasterConnectivityError(err, config.MasterAddr) {
								log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
								exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
							}
							log.Printf("Failed to reseed dynamic graph %s: %v", graphName, err)
						}
					} else {
						log.Printf("Dynamic graph %s already seeded (%d nodes)", graphName, count)
					}
				} else {
					// Full population mode
					threshold := config.TargetNodesPerGraph / 2
					if count < threshold {
						log.Printf("Dynamic graph %s has only %d nodes (target: %d), repopulating...", graphName, count, config.TargetNodesPerGraph)
						if err := populateGraph(ctx, masterClient, graphName, config.TargetNodesPerGraph, config.NumNodeTypes, config.NumLabelsPerType); err != nil {
							if isMasterConnectivityError(err, config.MasterAddr) {
								log.Printf("FATAL: Lost contact with master (%s): %v. Exiting...", config.MasterAddr, err)
								exitOnMasterConnectivityLoss(err, config.MasterAddr, config)
							}
							log.Printf("Failed to repopulate dynamic graph %s: %v", graphName, err)
						}
					} else {
						log.Printf("Dynamic graph %s already populated (%d nodes)", graphName, count)
					}
				}
			}
		}
	}

	log.Printf("All graphs checked/populated.")

	// Create indexes on _updated and name properties to increase lock contention and RediSearch GC activity
	if err := createIndexes(ctx, masterClient, config); err != nil {
		log.Printf("Warning: Failed to create indexes: %v", err)
		// Continue anyway - indexes are optional for the test
	}

	log.Printf("Starting stress test...")
	return nil
}

// createIndexes creates indexes on _updated and name properties for all graphs
// This increases lock contention and RediSearch GC activity
func createIndexes(ctx context.Context, client *redis.Client, config Config) error {
	log.Printf("Creating indexes on _updated and name properties for %d regular graphs and %d dynamic graphs...", config.NumGraphs, config.NumGraphs)

	// Helper function to create an index on a property
	createIndexOnProperty := func(graphName, property string) error {
		indexQuery := fmt.Sprintf("CREATE INDEX FOR (n:Node) ON (n.%s)", property)
		_, err := client.Do(ctx, "GRAPH.QUERY", graphName, indexQuery).Result()
		if err != nil {
			// Check if index already exists (that's okay)
			errStr := fmt.Sprintf("%v", err)
			if strings.Contains(errStr, "already exists") || strings.Contains(errStr, "already exist") {
				return nil // Index exists, that's fine
			}
			return err
		}
		return nil
	}

	// Create indexes for regular graphs
	for i := 0; i < config.NumGraphs; i++ {
		graphName := fmt.Sprintf("graph-%d", i)

		// Create index on _updated property
		if err := createIndexOnProperty(graphName, "_updated"); err != nil {
			log.Printf("Warning: Failed to create index on _updated for %s: %v", graphName, err)
		} else {
			log.Printf("Created index on _updated for %s", graphName)
		}
		time.Sleep(10 * time.Millisecond)

		// Create index on name property (increases RediSearch activity)
		if err := createIndexOnProperty(graphName, "name"); err != nil {
			log.Printf("Warning: Failed to create index on name for %s: %v", graphName, err)
		} else {
			log.Printf("Created index on name for %s", graphName)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Create indexes for dynamic graphs
	for i := 0; config.NumDynamicGraphWorkers > 0 && i < config.NumGraphs; i++ {
		graphName := fmt.Sprintf("dynamic-graph-%d", i)

		// Create index on _updated property
		if err := createIndexOnProperty(graphName, "_updated"); err != nil {
			log.Printf("Warning: Failed to create index on _updated for %s: %v", graphName, err)
		} else {
			log.Printf("Created index on _updated for %s", graphName)
		}
		time.Sleep(10 * time.Millisecond)

		// Create index on name property (increases RediSearch activity)
		if err := createIndexOnProperty(graphName, "name"); err != nil {
			log.Printf("Warning: Failed to create index on name for %s: %v", graphName, err)
		} else {
			log.Printf("Created index on name for %s", graphName)
		}
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Index creation completed")
	return nil
}

// randomJitter returns a random duration between 0 and maxMs milliseconds to desynchronize workers
func randomJitter(maxMs int) time.Duration {
	return time.Duration(rand.Intn(maxMs)) * time.Millisecond
}

func updateWorker(ctx context.Context, client *redis.Client, config Config, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(config.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			// Add random jitter to desynchronize workers
			time.Sleep(randomJitter(config.JitterMaxMs))

			// Pick a random graph
			graphNum := rand.Intn(config.NumGraphs)
			graphName := fmt.Sprintf("graph-%d", graphNum)

			now := time.Now().Unix()
			nowMs := time.Now().UnixMilli()

			// Production pattern: Sometimes update nodes, sometimes update relationships with _expired
			// Randomly choose between different update patterns to match production behavior
			updatePattern := rand.Float32()

			if updatePattern < 0.4 {
				// Pattern 1: Update node _updated timestamps (40% of the time)
				var updateNodesQuery string
				if config.UpdateNodeCount > 0 {
					// Update a specific number of nodes using LIMIT
					updateNodesQuery = fmt.Sprintf("MATCH (n) WITH n LIMIT %d SET n._updated = %d", config.UpdateNodeCount, now)
				} else {
					// Update all nodes (default behavior)
					updateNodesQuery = fmt.Sprintf("MATCH (n) SET n._updated = %d", now)
				}

				_, err := client.Do(ctx, "GRAPH.QUERY", graphName, updateNodesQuery).Result()
				if err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
					}
					if isBusyError(err) {
						n := busyErrorCount.Add(1)
						log.Printf("🚨 BUSY error on master/%s (total: %d) op=updateWorker/SET_nodes: %v", graphName, n, err)
						continue
					}
					log.Printf("Update nodes failed for %s: %v", graphName, err)
					continue
				}
				updateType := "all"
				if config.UpdateNodeCount > 0 {
					updateType = fmt.Sprintf("%d", config.UpdateNodeCount)
				}
				log.Printf("Updated %s nodes in %s", updateType, graphName)
			} else if updatePattern < 0.7 {
				// Pattern 2: Update relationship _updated timestamps (30% of the time)
				var updateRelsQuery string
				if config.UpdateNodeCount > 0 {
					// Update relationships for a limited number of nodes
					updateRelsQuery = fmt.Sprintf("MATCH (n)-[r]->(m) WITH n, r, m LIMIT %d SET r._updated = %d", config.UpdateNodeCount, now)
				} else {
					// Update all relationships
					updateRelsQuery = fmt.Sprintf("MATCH (n)-[r]->(m) SET r._updated = %d", now)
				}
				_, err := client.Do(ctx, "GRAPH.QUERY", graphName, updateRelsQuery).Result()
				if err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
					}
					if isBusyError(err) {
						n := busyErrorCount.Add(1)
						log.Printf("🚨 BUSY error on master/%s (total: %d) op=updateWorker/SET_rels: %v", graphName, n, err)
						continue
					}
					// It's okay if there are no relationships, just log it
					log.Printf("Update relationships failed for %s (may have no relationships): %v", graphName, err)
				} else {
					updateType := "all"
					if config.UpdateNodeCount > 0 {
						updateType = fmt.Sprintf("%d", config.UpdateNodeCount)
					}
					log.Printf("Updated %s relationships in %s", updateType, graphName)
				}
			} else {
				// Pattern 3: Production pattern - scan relationships and set _expired based on _updated (30% of the time)
				// Similar to production query: "MATCH (start)-[r]->(end) WHERE r._rel_source IS NULL AND r._expired IS NULL AND r._updated < X SET r._expired=Y"
				// We'll use a threshold: expire relationships that haven't been updated in the last 5 minutes
				thresholdMs := nowMs - 300000 // 5 minutes ago
				expiredValue := nowMs

				// Production pattern: scan all relationships and expire old ones
				// This is a slow operation that scans all relationships (similar to 751ms production query)
				expireRelsQuery := fmt.Sprintf(
					"MATCH (start)-[r]->(end) WHERE r._expired IS NULL AND r._updated < %d SET r._expired = %d RETURN count(r) as expiredCount",
					thresholdMs, expiredValue)

				result, err := client.Do(ctx, "GRAPH.QUERY", graphName, expireRelsQuery).Result()
				if err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
					}
					if isBusyError(err) {
						n := busyErrorCount.Add(1)
						log.Printf("🚨 BUSY error on master/%s (total: %d) op=updateWorker/expire_rels: %v", graphName, n, err)
						continue
					}
					log.Printf("Expire relationships failed for %s: %v", graphName, err)
				} else {
					// Try to extract the count from result
					expiredCount := "unknown"
					if resultArray, ok := result.([]interface{}); ok && len(resultArray) >= 2 {
						if dataRows, ok := resultArray[1].([]interface{}); ok && len(dataRows) > 0 {
							if row, ok := dataRows[0].([]interface{}); ok && len(row) > 0 {
								expiredCount = fmt.Sprintf("%v", row[0])
							}
						}
					}
					log.Printf("Expired %s relationships in %s (updated < %d)", expiredCount, graphName, thresholdMs)
				}
			}
		}
	}
}

func dynamicGraphWorker(ctx context.Context, client *redis.Client, config Config, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(config.DynamicGraphInterval)
	defer ticker.Stop()

	// Use a small subset of dynamic graphs to avoid uncontrolled growth
	// Cycle through NUM_GRAPHS dynamic graphs
	maxDynamicGraphs := config.NumGraphs

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			// Randomly decide whether to recreate a graph (50% chance)
			if rand.Float32() < 0.5 {
				continue
			}

			// Add random jitter to desynchronize workers
			time.Sleep(randomJitter(config.JitterMaxMs))

			// Use GRAPH.LIST to get current graphs (this uses GraphIterator_Next which acquires read lock)
			result, err := client.Do(ctx, "GRAPH.LIST").Result()
			var graphName string

			if err != nil {
				if isMasterConnectivityError(err, config.MasterAddr) {
					handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
				}
				log.Printf("Warning: GRAPH.LIST failed: %v, falling back to random selection", err)
				// Fallback to random selection if GRAPH.LIST fails
				graphNum := rand.Intn(maxDynamicGraphs)
				graphName = fmt.Sprintf("dynamic-graph-%d", graphNum)
			} else {
				// Parse GRAPH.LIST response (array of graph names)
				graphList, ok := result.([]interface{})
				if !ok {
					log.Printf("Warning: GRAPH.LIST returned unexpected type, falling back to random selection")
					graphNum := rand.Intn(maxDynamicGraphs)
					graphName = fmt.Sprintf("dynamic-graph-%d", graphNum)
				} else {
					// Filter for dynamic-graph-* graphs
					var dynamicGraphs []string
					for _, g := range graphList {
						if name, ok := g.(string); ok && strings.HasPrefix(name, "dynamic-graph-") {
							dynamicGraphs = append(dynamicGraphs, name)
						}
					}

					if len(dynamicGraphs) > 0 {
						// Randomly choose from existing dynamic graphs
						graphName = dynamicGraphs[rand.Intn(len(dynamicGraphs))]
					} else {
						// No dynamic graphs exist yet, create a new one
						graphNum := rand.Intn(maxDynamicGraphs)
						graphName = fmt.Sprintf("dynamic-graph-%d", graphNum)
					}
				}
			}

			// Check if graph exists and has enough nodes
			nodeCount, err := getNodeCount(ctx, client, graphName)
			if err != nil {
				// Check if error is because graph doesn't exist
				errStr := err.Error()
				if strings.Contains(errStr, "not found") ||
					strings.Contains(errStr, "does not exist") ||
					strings.Contains(errStr, "Unknown graph") ||
					strings.Contains(errStr, "empty key") {
					// Graph doesn't exist, skip (let ensureGraphsPopulated handle creation)
					log.Printf("Graph %s doesn't exist, skipping", graphName)
					continue
				} else {
					// Other error - skip
					if isMasterConnectivityError(err, config.MasterAddr) {
						handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
					}
					log.Printf("Graph %s exists but couldn't get node count (%v), skipping", graphName, err)
					continue
				}
			}

			// Only proceed if graph has enough nodes to delete
			if nodeCount < config.DynamicGraphNodeCount {
				log.Printf("Graph %s has only %d nodes (need %d), skipping", graphName, nodeCount, config.DynamicGraphNodeCount)
				continue
			}

			// Delete configurable number of nodes (this triggers graph operations and lock contention)
			deleteCount := config.DynamicGraphNodeCount
			deleteQuery := fmt.Sprintf("MATCH (n) WITH n LIMIT %d DELETE n", deleteCount)
			_, err = client.Do(ctx, "GRAPH.QUERY", graphName, deleteQuery).Result()
			if err != nil {
				if isMasterConnectivityError(err, config.MasterAddr) {
					handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
				}
				// Graph might not exist or have nodes, that's okay
				errStr := err.Error()
				if strings.Contains(errStr, "not found") ||
					strings.Contains(errStr, "does not exist") ||
					strings.Contains(errStr, "Unknown graph") ||
					strings.Contains(errStr, "empty key") {
					// Graph doesn't exist, skip silently
					continue
				}
				log.Printf("Dynamic graph worker: Failed to delete nodes from %s: %v", graphName, err)
				continue
			}

			// Recreate nodes with double-linked list structure
			// Find the last node to link from (or create a seed if needed)
			now := time.Now().Unix()
			seedQuery := "MATCH (n) WITH n ORDER BY n.name DESC LIMIT 1 RETURN n.name as lastName"
			seedResult, err := client.Do(ctx, "GRAPH.QUERY", graphName, seedQuery).Result()
			var lastNodeName string
			if err == nil {
				// Parse result to get last node name
				if resultArray, ok := seedResult.([]interface{}); ok && len(resultArray) >= 2 {
					if dataRows, ok := resultArray[1].([]interface{}); ok && len(dataRows) > 0 {
						if row, ok := dataRows[0].([]interface{}); ok && len(row) > 0 {
							if name, ok := row[0].(string); ok {
								lastNodeName = name
							}
						}
					}
				}
			}

			// If we couldn't find a last node, create a seed node first using MERGE pattern
			if lastNodeName == "" {
				seedNodeName := fmt.Sprintf("seed-%d", now)
				nowMs := time.Now().UnixMilli()
				seedMergeQuery := fmt.Sprintf(
					"MERGE (n:Node {name: '%s', scope_id: 'seed-scope'}) "+
						"ON CREATE SET n._created = %d "+
						"ON MATCH SET n._expired = null "+
						"SET n.prop1 = 'seed', n.prop2 = 'seed', n.prop3 = 'seed', "+
						"n.prop4 = 'seed', n.prop5 = 'seed', n.prop6 = 'seed', "+
						"n.prop7 = 'seed', n.prop8 = 'seed', n.prop9 = 'seed', "+
						"n.prop10 = 'seed', n._updated = %d "+
						"RETURN n.name",
					seedNodeName, nowMs, nowMs)
				_, err := client.Do(ctx, "GRAPH.QUERY", graphName, seedMergeQuery).Result()
				if err != nil {
					log.Printf("Dynamic graph worker: Failed to create seed node in %s: %v", graphName, err)
					continue
				}
				lastNodeName = seedNodeName
			}

			// Create new nodes and link them in a chain using MERGE pattern similar to production
			for i := 0; i < deleteCount; i++ {
				nodeName := fmt.Sprintf("dynamic-node-%d-%d", now, i)
				// Use MERGE pattern similar to production query with generic properties
				// MERGE on name and scope_id (generic scope-like property)
				scopeId := fmt.Sprintf("scope-%d", i%10) // Cycle through 10 different scopes
				nowMs := time.Now().UnixMilli()
				mergeNodeAndLinkQuery := fmt.Sprintf(
					"MATCH (prev:Node {name: '%s'}) "+
						"MERGE (n:Node {name: '%s', scope_id: '%s'}) "+
						"ON CREATE SET n._created = %d "+
						"ON MATCH SET n._expired = null "+
						"SET n.prop1 = 'value%d', n.prop2 = 'value%d', n.prop3 = 'value%d', "+
						"n.prop4 = 'value%d', n.prop5 = 'value%d', n.prop6 = 'value%d', "+
						"n.prop7 = 'value%d', n.prop8 = 'value%d', n.prop9 = 'value%d', "+
						"n.prop10 = 'value%d', n._updated = %d "+
						"CREATE (prev)-[:NEXT]->(n), (n)-[:PREV]->(prev) "+
						"RETURN ID(n)",
					lastNodeName, nodeName, scopeId, nowMs, i, i, i, i, i, i, i, i, i, i, nowMs)
				_, err := client.Do(ctx, "GRAPH.QUERY", graphName, mergeNodeAndLinkQuery).Result()
				if err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
					}
					log.Printf("Dynamic graph worker: Failed to create node %s in %s: %v", nodeName, graphName, err)
					break
				}
				// Update lastNodeName for next iteration
				lastNodeName = nodeName
			}

			log.Printf("Dynamic graph worker: Deleted and recreated %d nodes in %s", deleteCount, graphName)
		}
	}
}

func queryWorker(ctx context.Context, masterClient *redis.Client, replicaClients []*redis.Client, replicaAddrs []string, allowedReplicas []string, config Config, workerID int, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(config.QueryInterval)
	defer ticker.Stop()

	// Build a map of allowed replica addresses for quick lookup
	allowedReplicaMap := make(map[string]bool)
	for _, addr := range allowedReplicas {
		allowedReplicaMap[addr] = true
	}

	// Build list of allowed clients (master + filtered replicas)
	type clientInfo struct {
		client *redis.Client
		name   string
	}
	allowedClients := []clientInfo{
		{client: masterClient, name: "master"},
	}

	// Add replicas that are in the allowed list
	for i, replicaClient := range replicaClients {
		replicaAddr := replicaAddrs[i]
		if allowedReplicaMap[replicaAddr] {
			replicaName := fmt.Sprintf("replica-%d", i+1)
			if len(replicaClients) == 1 {
				replicaName = "replica"
			}
			allowedClients = append(allowedClients, clientInfo{client: replicaClient, name: replicaName})
		}
	}

	// Array of queries that visit nodes and check _created and/or _expired properties
	// Workers randomly select one query per iteration
	// Queries can use $timestamp placeholder which will be replaced with current timestamp (milliseconds)
	queries := []string{
		// Simple query: just visit all nodes and check properties
		// "MATCH (n) RETURN n._created, n._expired, n.name, n._updated ORDER BY n._created",
		// Production pattern: DISTINCT + COUNT on relationships with _expired/_created filters (similar to 1710ms query)
		// Enhanced with more complex operations and longer timestamp window to increase lock hold time
		// Uses 24 hour window (86400000ms) to match more relationships and force longer processing
		"MATCH (a)-[r]->(b) WHERE ((r._expired IS NULL OR r._expired >= ($timestamp - 86400000)) AND r._created <= ($timestamp + 86400000)) WITH r, type(r) as relType, a, b, toString(r._created) + '-' + toString(r._expired) as relProps, abs(r._updated - r._created) as relAge WHERE size(relProps) > 0 RETURN DISTINCT relType, COUNT(relType) as count, avg(relAge) as avgAge, min(relAge) as minAge, max(relAge) as maxAge ORDER BY count DESC",
		// Slow query: visits all nodes and performs cumulative hash calculation
		"MATCH (n) WITH n ORDER BY n.name WITH n, n._updated + n._created as nodeSum, (n._updated * 31 + n._created * 7) % 1000000 as nodeHash, abs(n._updated - n._created) as nodeDiff, (n._updated * n._created) % 10000000 as nodeProduct, (n._updated * 17 + n._created * 13) % 5000000 as nodeHash2, (n._updated % 1000) * (n._created % 1000) as nodeHash3 RETURN sum(nodeHash) as cumulativeHash, count(*) as nodeCount, avg(n._updated) as avgUpdated, sum(nodeSum) as totalNodeSum, min(n._created) as minCreated, max(n._updated) as maxUpdated, avg(nodeDiff) as avgDiff, sum(nodeProduct) as totalProduct, sum(nodeHash2) as cumulativeHash2, sum(nodeHash3) as cumulativeHash3",
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			// Add random jitter to desynchronize workers
			time.Sleep(randomJitter(config.JitterMaxMs))

			// Randomly choose from allowed clients (master + allowed replicas)
			selected := allowedClients[rand.Intn(len(allowedClients))]
			client := selected.client
			clientName := selected.name

			// Randomly select a query from the array
			queryIdx := rand.Intn(len(queries))
			query := queries[queryIdx]

			// Use GRAPH.LIST to get all graphs (this acquires read lock)
			result, err := client.Do(ctx, "GRAPH.LIST").Result()
			if err != nil {
				// Check if it's a connectivity error to master
				if clientName == "master" && isMasterConnectivityError(err, config.MasterAddr) {
					handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
				}
				log.Printf("Query worker %d: GRAPH.LIST failed on %s: %v", workerID, clientName, err)
				continue
			}

			// Parse GRAPH.LIST response (array of graph names)
			graphList, ok := result.([]interface{})
			if !ok {
				log.Printf("Query worker %d: GRAPH.LIST returned unexpected type on %s", workerID, clientName)
				continue
			}

			// Filter for regular graphs (graph-*) and optionally dynamic graphs
			var graphsToQuery []string
			for _, g := range graphList {
				if name, ok := g.(string); ok {
					if strings.HasPrefix(name, "graph-") {
						graphsToQuery = append(graphsToQuery, name)
					}
					// Optionally include dynamic graphs
					if strings.HasPrefix(name, "dynamic-graph-") {
						graphsToQuery = append(graphsToQuery, name)
					}
				}
			}

			if len(graphsToQuery) == 0 {
				log.Printf("Query worker %d: No graphs found to query on %s", workerID, clientName)
				continue
			}

			// Walk through each graph and execute the selected query
			for _, graphName := range graphsToQuery {
				// Use RedisGraph parameterized queries with CYPHER clause
				// All queries can use $timestamp parameter whether they need it or not
				timestampMs := time.Now().UnixMilli()

				// Build parameterized query: if query contains $timestamp, use CYPHER clause
				var execQuery string
				if strings.Contains(query, "$timestamp") {
					// Use CYPHER clause format: "CYPHER timestamp=value query"
					// The query can reference $timestamp directly - CYPHER clause sets the parameter
					execQuery = fmt.Sprintf("CYPHER timestamp=%d %s", timestampMs, query)
				} else {
					// Query doesn't use timestamp, execute as-is
					execQuery = query
				}

				// Execute the query using GRAPH.RO_QUERY (read-only, works on both master and replica)
				// This query visits all nodes and checks _created/_expired properties
				startTime := time.Now()
				_, err := client.Do(ctx, "GRAPH.RO_QUERY", graphName, execQuery).Result()
				duration := time.Since(startTime)

				if err != nil {
					// Check if it's a connectivity error to master
					if clientName == "master" && isMasterConnectivityError(err, config.MasterAddr) {
						handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
					}
					// Check if graph doesn't exist (that's okay for dynamic graphs - may have been deleted by another worker)
					errStr := err.Error()
					if strings.Contains(errStr, "not found") ||
						strings.Contains(errStr, "does not exist") ||
						strings.Contains(errStr, "Unknown graph") ||
						strings.Contains(errStr, "empty key") ||
						strings.Contains(errStr, "Invalid graph operation on empty key") {
						// Graph doesn't exist, skip silently (expected for dynamic graphs)
						continue
					}
					if isBusyError(err) {
						n := busyErrorCount.Add(1)
						log.Printf("🚨 BUSY error on %s/%s (total: %d) op=queryWorker/RO_QUERY[%d]: %v", clientName, graphName, n, queryIdx, err)
						continue
					}
					log.Printf("Query worker %d: Query[%d] failed on %s for %s (took %v): %v", workerID, queryIdx, clientName, graphName, duration, err)
				} else {
					// log.Printf("Query worker %d: Query[%d] executed on %s for %s (took %v)", workerID, queryIdx, clientName, graphName, duration)
				}
			}
		}
	}
}

func gcGarbageWorker(ctx context.Context, client *redis.Client, config Config, workerID int, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(config.GCGarbageInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			// Add random jitter to desynchronize workers
			time.Sleep(randomJitter(config.JitterMaxMs))

			// Pick a random graph (regular graphs only, to avoid conflicts with dynamic graph worker)
			graphNum := rand.Intn(config.NumGraphs)
			graphName := fmt.Sprintf("graph-%d", graphNum)

			// Delete configurable number of nodes to create garbage for RediSearch GC
			// Each node has 2 indexes (_updated and name), so deleting 1 node = 2 deleted docs
			// To reliably hit the 100-doc threshold, we need at least 50 nodes deleted (creates 100+ deleted docs)
			// Use WITH ... LIMIT ... DELETE pattern (LIMIT must come before DELETE in Cypher)
			deleteCount := config.GCGarbageNodeCount
			deletedDocs := deleteCount * 2 // Each node has 2 indexed properties
			deleteQuery := fmt.Sprintf("MATCH (n) WITH n LIMIT %d DELETE n", deleteCount)
			_, err := client.Do(ctx, "GRAPH.QUERY", graphName, deleteQuery).Result()
			if err != nil {
				if isMasterConnectivityError(err, config.MasterAddr) {
					handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
				}
				// Graph might not exist or have nodes, that's okay
				errStr := err.Error()
				if strings.Contains(errStr, "not found") ||
					strings.Contains(errStr, "does not exist") ||
					strings.Contains(errStr, "Unknown graph") ||
					strings.Contains(errStr, "empty key") {
					// Graph doesn't exist, skip silently
					continue
				}
				if isBusyError(err) {
					n := busyErrorCount.Add(1)
					log.Printf("🚨 BUSY error on master/%s (total: %d) op=gcGarbageWorker/DELETE_nodes: %v", graphName, n, err)
					continue
				}
				log.Printf("GC garbage worker %d: Failed to delete nodes from %s: %v", workerID, graphName, err)
				continue
			}

			// Recreate nodes with different names to keep graph size stable
			// Create nodes with a timestamp-based name to ensure uniqueness
			// Use the same count as deleted to maintain graph size
			now := time.Now().UnixNano()
			recreateCount := deleteCount // Use same count as deleted nodes
			for i := 0; i < recreateCount; i++ {
				nodeName := fmt.Sprintf("gc-node-%d-%d", now, i)
				createQuery := fmt.Sprintf("CREATE (n:Node {name: '%s', _updated: %d})", nodeName, now)
				_, err := client.Do(ctx, "GRAPH.QUERY", graphName, createQuery).Result()
				if err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
					}
					if isBusyError(err) {
						n := busyErrorCount.Add(1)
						log.Printf("🚨 BUSY error on master/%s (total: %d) op=gcGarbageWorker/CREATE_nodes: %v", graphName, n, err)
						break
					}
					// Log but continue - some failures are expected
					log.Printf("GC garbage worker %d: Failed to recreate node in %s: %v", workerID, graphName, err)
					break
				}
			}

			log.Printf("GC garbage worker %d: Deleted %d nodes (%d deleted docs) and recreated in %s - should trigger RediSearch GC", workerID, deleteCount, deletedDocs, graphName)
		}
	}
}

func expireWorker(ctx context.Context, client *redis.Client, config Config, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(config.ExpireInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			graphNum := rand.Intn(config.NumGraphs)
			graphName := fmt.Sprintf("graph-%d", graphNum)
			now := time.Now().UnixMilli()
			q := fmt.Sprintf(
				"MATCH (n:Node) WHERE n._expired IS NULL WITH n LIMIT %d SET n._expired = %d",
				config.ExpireNodeCount, now)
			_, err := client.Do(ctx, "GRAPH.QUERY", graphName, q).Result()
			if err != nil {
				if isMasterConnectivityError(err, config.MasterAddr) {
					handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
				}
				if isBusyError(err) {
					n := busyErrorCount.Add(1)
					log.Printf("🚨 BUSY error on master/%s (total: %d) op=expireWorker/SET_expired: %v", graphName, n, err)
					continue
				}
				log.Printf("expireWorker: failed to mark nodes expired in %s: %v", graphName, err)
				continue
			}
			log.Printf("expireWorker: marked up to %d nodes _expired in %s", config.ExpireNodeCount, graphName)
		}
	}
}

func repopulateWorker(ctx context.Context, client *redis.Client, config Config, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(config.ExpireInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			graphNum := rand.Intn(config.NumGraphs)
			graphName := fmt.Sprintf("graph-%d", graphNum)
			typeIdx := rand.Intn(config.NumNodeTypes)
			typeLabels := buildNodeTypeLabels(typeIdx, config.NumLabelsPerType)
			now := time.Now().Unix()
			total := config.ExpireNodeCount
			created := 0
			const batchSize = 500
			for created < total {
				batch := batchSize
				if batch > total-created {
					batch = total - created
				}
				base := int(repopulateNodeCounter.Add(int64(batch))) - batch
				q := fmt.Sprintf(
					"UNWIND range(0, %d) AS i CREATE (n%s {name: 'rp-'+toString(%d+i), _created: %d, _updated: %d})",
					batch-1, typeLabels, base, now, now)
				_, err := client.Do(ctx, "GRAPH.QUERY", graphName, q).Result()
				if err != nil {
					if isMasterConnectivityError(err, config.MasterAddr) {
						handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
					}
					if isBusyError(err) {
						n := busyErrorCount.Add(1)
						log.Printf("🚨 BUSY error on master/%s (total: %d) op=repopulateWorker/CREATE: %v", graphName, n, err)
					} else {
						log.Printf("repopulateWorker: batch create failed in %s: %v", graphName, err)
					}
					break
				}
				created += batch
			}
			if created > 0 {
				log.Printf("repopulateWorker: created %d nodes in %s", created, graphName)
			}
		}
	}
}

func expireRelationsWorker(ctx context.Context, client *redis.Client, config Config, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(config.ExpireRelInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			graphNum := rand.Intn(config.NumGraphs)
			graphName := fmt.Sprintf("graph-%d", graphNum)
			threshold := time.Now().Unix() - int64(config.StaleRelThresholdSec)
			// No LIMIT — full relationship scan matching production's expirePropertyBasedRelations
			q := fmt.Sprintf(
				"MATCH ()-[r:CONNECTS_TO]->() WHERE r._relation_source IS NULL AND r._expired IS NULL AND r._updated < %d SET r._expired = %d",
				threshold, time.Now().UnixMilli())
			_, err := client.Do(ctx, "GRAPH.QUERY", graphName, q).Result()
			if err != nil {
				if isMasterConnectivityError(err, config.MasterAddr) {
					handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
				}
				if isBusyError(err) {
					n := busyErrorCount.Add(1)
					log.Printf("🚨 BUSY error on master/%s (total: %d) op=expireRelWorker/SET_expired: %v", graphName, n, err)
					continue
				}
				log.Printf("expireRelationsWorker: failed on %s: %v", graphName, err)
				continue
			}
			log.Printf("expireRelationsWorker: scanned all CONNECTS_TO in %s (threshold: %ds ago)", graphName, config.StaleRelThresholdSec)
		}
	}
}

func gcExpiredRun(ctx context.Context, client *redis.Client, config Config, cancel context.CancelFunc) {
	for i := 0; i < config.NumGraphs; i++ {
		graphName := fmt.Sprintf("graph-%d", i)
		q := "MATCH (n:Node) WHERE n._expired IS NOT NULL WITH n LIMIT 100000 DETACH DELETE n"
		_, err := client.Do(ctx, "GRAPH.QUERY", graphName, q).Result()
		if err != nil {
			if isMasterConnectivityError(err, config.MasterAddr) {
				handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
			}
			if isBusyError(err) {
				n := busyErrorCount.Add(1)
				log.Printf("🚨 BUSY error on master/%s (total: %d) op=gcExpiredWorker/DETACH_DELETE: %v", graphName, n, err)
				continue
			}
			log.Printf("gcExpiredWorker: failed to delete expired nodes from %s: %v", graphName, err)
			continue
		}
		log.Printf("gcExpiredWorker: deleted expired nodes from %s", graphName)
	}
}

func gcExpiredWorker(ctx context.Context, client *redis.Client, config Config, cancel context.CancelFunc, done chan struct{}) {
	// Sleep until GCPreBGSAVEOffset before the first BGSAVE fires, then run
	// immediately (not after another full BGSAVEInterval).
	offset := config.BGSAVEInterval - config.GCPreBGSAVEOffset
	if offset > 0 {
		select {
		case <-time.After(offset):
		case <-ctx.Done():
			return
		case <-done:
			return
		}
	}

	// First run: fires GCPreBGSAVEOffset seconds before the first BGSAVE
	gcExpiredRun(ctx, client, config, cancel)

	ticker := time.NewTicker(config.BGSAVEInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			gcExpiredRun(ctx, client, config, cancel)
		}
	}
}

// checkBGSAVEStatus checks the BGSAVE status using INFO persistence
func checkBGSAVEStatus(ctx context.Context, client *redis.Client, instanceName string) {
	info, err := client.Info(ctx, "persistence").Result()
	if err != nil {
		return // Silently fail - not critical
	}

	// Parse rdb_bgsave_in_progress and rdb_last_bgsave_time_sec
	lines := strings.Split(info, "\n")
	var bgsaveInProgress bool
	var lastBGSaveTimeSec int64 = -1
	var bgsaveStartTime int64 = -1

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "rdb_bgsave_in_progress:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				if strings.TrimSpace(parts[1]) == "1" {
					bgsaveInProgress = true
				}
			}
		} else if strings.HasPrefix(line, "rdb_last_bgsave_time_sec:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				if parsed, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
					lastBGSaveTimeSec = parsed
				}
			}
		} else if strings.HasPrefix(line, "rdb_current_bgsave_time_sec:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				if parsed, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
					bgsaveStartTime = parsed
				}
			}
		}
	}

	if bgsaveInProgress {
		if bgsaveStartTime > 0 {
			// BGSAVE is in progress and we know how long it's been running
			if bgsaveStartTime > 30 {
				log.Printf("⚠️  BGSAVE on %s has been running for %d seconds - possible hang!", instanceName, bgsaveStartTime)
			}
		} else {
			log.Printf("BGSAVE in progress on %s (duration unknown)", instanceName)
		}
	} else if lastBGSaveTimeSec > 0 {
		// Last BGSAVE completed, log if it took a long time
		if lastBGSaveTimeSec > 30 {
			log.Printf("Last BGSAVE on %s took %d seconds", instanceName, lastBGSaveTimeSec)
		}
	}
}

func bgsaveWorker(ctx context.Context, masterClient *redis.Client, replicaClients []*redis.Client, config Config, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(config.BGSAVEInterval)
	defer ticker.Stop()

	// Track consecutive blocked BGSAVE attempts per instance
	// Only log 🚨 BLOCKED after 10 consecutive failures
	BLOCKED_THRESHOLD := 10
	blockedCounters := make(map[string]int)

	// Also check BGSAVE status periodically to detect long-running BGSAVEs
	statusTicker := time.NewTicker(5 * time.Second)
	defer statusTicker.Stop()

	// Helper function to handle BGSAVE result for an instance
	handleBGSaveResult := func(instanceName string, result interface{}, err error) {
		if err != nil {
			// Check for connectivity errors to master
			if instanceName == "master" && isMasterConnectivityError(err, config.MasterAddr) {
				handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
			}
			errStr := err.Error()
			// Check for "can't BGSAVE right now" - this indicates a child process is blocking BGSAVE
			if strings.Contains(errStr, "can't BGSAVE right now") || strings.Contains(errStr, "Another child process is active") {
				blockedCounters[instanceName]++
				count := blockedCounters[instanceName]
				if count >= BLOCKED_THRESHOLD {
					log.Printf("🚨 BLOCKED: BGSAVE blocked on %s (child process active) - %d consecutive failures - possible hang! Error: %v", instanceName, count, err)
				} else {
					// Don't log false positives, just track silently
				}
			} else {
				// Other errors reset the counter
				blockedCounters[instanceName] = 0
				log.Printf("BGSAVE failed on %s: %v", instanceName, err)
			}
		} else {
			// Successful BGSAVE resets the counter
			blockedCounters[instanceName] = 0
			// Check the response message
			if resultStr, ok := result.(string); ok {
				if strings.Contains(resultStr, "already in progress") {
					log.Printf("⚠️  WARNING: BGSAVE already in progress on %s - possible hang detected! Response: %s", instanceName, resultStr)
				} else {
					log.Printf("BGSAVE initiated on %s: %s", instanceName, resultStr)
				}
			} else {
				log.Printf("BGSAVE initiated on %s (response: %v)", instanceName, result)
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-statusTicker.C:
			// Periodically check BGSAVE status to detect long-running BGSAVEs
			checkBGSAVEStatus(ctx, masterClient, "master")
			for i, replicaClient := range replicaClients {
				replicaName := fmt.Sprintf("replica-%d", i+1)
				if len(replicaClients) == 1 {
					replicaName = "replica"
				}
				checkBGSAVEStatus(ctx, replicaClient, replicaName)
			}
		case <-ticker.C:
			// Add random jitter to desynchronize BGSAVE triggers
			time.Sleep(randomJitter(config.JitterMaxMs))

			// Trigger BGSAVE on master
			log.Printf("Triggering BGSAVE on master...")
			result, err := masterClient.Do(ctx, "BGSAVE").Result()
			handleBGSaveResult("master", result, err)

			// Trigger BGSAVE on all replicas
			for i, replicaClient := range replicaClients {
				// Add small jitter between replicas
				time.Sleep(randomJitter(config.JitterMaxMs))

				replicaName := fmt.Sprintf("replica-%d", i+1)
				if len(replicaClients) == 1 {
					replicaName = "replica"
				}
				log.Printf("Triggering BGSAVE on %s...", replicaName)
				result, err := replicaClient.Do(ctx, "BGSAVE").Result()
				handleBGSaveResult(replicaName, result, err)
			}
		}
	}
}

// gcMonitorWorker periodically checks Redis INFO for RediSearch GC activity
// This helps track when GC is actually running (which creates the thread-pool-0 zombie)
func gcMonitorWorker(ctx context.Context, masterClient *redis.Client, config Config, cancel context.CancelFunc, done chan struct{}) {
	ticker := time.NewTicker(60 * time.Second) // Check every 60 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			// Get Redis INFO from multiple sections to check for RediSearch/GC statistics
			// Check "all" section first for comprehensive info
			infoAll, err := masterClient.Info(ctx, "all").Result()
			if err != nil {
				if isMasterConnectivityError(err, config.MasterAddr) {
					handleMasterConnectivityLoss(err, config.MasterAddr, config, cancel)
				}
				// Silently continue on other errors
				continue
			}

			// Also check modules section specifically
			infoModules, err := masterClient.Info(ctx, "modules").Result()
			if err == nil {
				infoAll += "\n" + infoModules
			}

			// Look for RediSearch GC indicators in the INFO output
			// RediSearch typically reports GC stats in various sections
			lines := strings.Split(infoAll, "\n")
			gcFound := false
			for _, line := range lines {
				lineLower := strings.ToLower(strings.TrimSpace(line))
				// Look for specific GC-related keywords (avoid bare "gc" which matches gcc_version etc.)
				if strings.Contains(lineLower, "search_gc") ||
					strings.Contains(lineLower, "fork_gc") ||
					strings.Contains(lineLower, "garbage") ||
					strings.Contains(lineLower, "thread-pool") {
					log.Printf("🔍 GC Monitor: %s", strings.TrimSpace(line))
					gcFound = true
				}
			}
			// Log summary if GC activity was detected
			if gcFound {
				log.Printf("🔍 GC Monitor: RediSearch GC activity detected in INFO output")
			}

			// Try to query RediSearch directly for index info (which might show GC activity)
			// Query a sample graph's index to see if we can detect GC-related info
			if rand.Float32() < 0.1 { // Only check occasionally (10% of the time) to avoid spam
				graphName := fmt.Sprintf("graph-%d", rand.Intn(config.NumGraphs))
				// Try FT.INFO on the graph's index if it exists
				// Note: This is a best-effort check, may not work depending on RedisGraph version
				_, err := masterClient.Do(ctx, "FT.INFO", graphName).Result()
				if err == nil {
					// If FT.INFO succeeds, we might be able to extract GC-related stats
					// For now, just log that we checked
				}
			}

			// Also check for background processes that might indicate GC is running
			// The thread-pool-0 zombie appears when GC forks a child process
			// We can't directly detect this from Redis, but verbose logging should show it
		}
	}
}

func main() {
	config := getConfig()
	log.Printf("Starting BGSAVE hang test driver")
	replicaInfo := config.ReplicaAddr
	if config.Replica2Addr != "" {
		replicaInfo = fmt.Sprintf("%s,%s", config.ReplicaAddr, config.Replica2Addr)
	}
	queryReplicasInfo := "none (master only)"
	if len(config.QueryReplicas) > 0 {
		queryReplicasInfo = strings.Join(config.QueryReplicas, ",")
	}
	log.Printf("Config: Master=%s, Replicas=%s, QueryReplicas=%s, Graphs=%d, NodesPerGraph=%d, NodeTypes=%d, LabelsPerType=%d, UpdateWorkers=%d, UpdateNodeCount=%d, DynamicGraphWorkers=%d, DynamicGraphNodeCount=%d, QueryWorkers=%d, GCGarbageWorkers=%d, GCGarbageNodeCount=%d, JitterMaxMs=%d, UpdateInterval=%v, DynamicGraphInterval=%v, QueryInterval=%v, GCGarbageInterval=%v, BGSAVEInterval=%v, ExpireWorkers=%d, ExpireInterval=%v, ExpireNodeCount=%d, GCExpiredWorkers=%d, GCPreBGSAVEOffset=%v",
		config.MasterAddr, replicaInfo, queryReplicasInfo, config.NumGraphs, config.TargetNodesPerGraph, config.NumNodeTypes, config.NumLabelsPerType, config.NumUpdateWorkers, config.UpdateNodeCount, config.NumDynamicGraphWorkers, config.DynamicGraphNodeCount, config.NumQueryWorkers, config.NumGCGarbageWorkers, config.GCGarbageNodeCount, config.JitterMaxMs, config.UpdateInterval, config.DynamicGraphInterval, config.QueryInterval, config.GCGarbageInterval, config.BGSAVEInterval, config.NumExpireWorkers, config.ExpireInterval, config.ExpireNodeCount, config.NumGCExpiredWorkers, config.GCPreBGSAVEOffset)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create Redis clients
	masterClient := createRedisClient(config.MasterAddr)
	defer masterClient.Close()

	replicaClient := createRedisClient(config.ReplicaAddr)
	defer replicaClient.Close()

	var replica2Client *redis.Client
	if config.Replica2Addr != "" {
		replica2Client = createRedisClient(config.Replica2Addr)
		defer replica2Client.Close()
	}

	// Wait for Redis to be ready
	log.Printf("Waiting for Redis to be ready...")
	for i := 0; i < 30; i++ {
		if err := masterClient.Ping(ctx).Err(); err == nil {
			log.Printf("Master Redis is ready")
			break
		}
		log.Printf("Waiting for master Redis... (attempt %d/30)", i+1)
		time.Sleep(1 * time.Second)
	}

	for i := 0; i < 30; i++ {
		if err := replicaClient.Ping(ctx).Err(); err == nil {
			log.Printf("Replica Redis is ready")
			break
		}
		log.Printf("Waiting for replica Redis... (attempt %d/30)", i+1)
		time.Sleep(1 * time.Second)
	}

	if replica2Client != nil {
		for i := 0; i < 30; i++ {
			if err := replica2Client.Ping(ctx).Err(); err == nil {
				log.Printf("Replica2 Redis is ready")
				break
			}
			log.Printf("Waiting for replica2 Redis... (attempt %d/30)", i+1)
			time.Sleep(1 * time.Second)
		}
	}

	// Ensure graphs are populated
	if err := ensureGraphsPopulated(ctx, masterClient, config); err != nil {
		log.Fatalf("Failed to populate graphs: %v", err)
	}

	// Trigger BGSAVE after population to persist data
	log.Printf("Population complete, triggering BGSAVE on all instances to persist data...")
	triggerBGSAVE(ctx, masterClient, "master")
	triggerBGSAVE(ctx, replicaClient, "replica")
	if replica2Client != nil {
		triggerBGSAVE(ctx, replica2Client, "replica2")
	}

	// Wait a moment for BGSAVE to start
	time.Sleep(1 * time.Second)

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start workers
	done := make(chan struct{})

	// Periodically log a running BUSY error total for easy comparison between versions
	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				log.Printf("📊 BUSY error total: %d", busyErrorCount.Load())
			}
		}
	}()
	// Start multiple update workers for concurrent updates
	for i := 0; i < config.NumUpdateWorkers; i++ {
		go updateWorker(ctx, masterClient, config, cancel, done)
	}
	// Start dynamic graph workers for create/delete operations
	for i := 0; i < config.NumDynamicGraphWorkers; i++ {
		go dynamicGraphWorker(ctx, masterClient, config, cancel, done)
	}
	// Build slice of replica clients and their addresses
	replicaClients := []*redis.Client{replicaClient}
	replicaAddrs := []string{config.ReplicaAddr}
	if replica2Client != nil {
		replicaClients = append(replicaClients, replica2Client)
		replicaAddrs = append(replicaAddrs, config.Replica2Addr)
	}

	// Start query workers for graph-walking queries
	for i := 0; i < config.NumQueryWorkers; i++ {
		go queryWorker(ctx, masterClient, replicaClients, replicaAddrs, config.QueryReplicas, config, i, cancel, done)
	}
	// Start GC garbage workers to create deleted documents for RediSearch GC
	for i := 0; i < config.NumGCGarbageWorkers; i++ {
		go gcGarbageWorker(ctx, masterClient, config, i, cancel, done)
	}
	go bgsaveWorker(ctx, masterClient, replicaClients, config, cancel, done)
	// Start GC monitor to track RediSearch GC activity
	go gcMonitorWorker(ctx, masterClient, config, cancel, done)
	// Start expire, repopulate, and GC-expired workers for production-shaped workload
	repopulateNodeCounter.Store(int64(config.TargetNodesPerGraph))
	for i := 0; i < config.NumExpireWorkers; i++ {
		go expireWorker(ctx, masterClient, config, cancel, done)
	}
	for i := 0; i < config.NumRepopulateWorkers; i++ {
		go repopulateWorker(ctx, masterClient, config, cancel, done)
	}
	for i := 0; i < config.NumGCExpiredWorkers; i++ {
		go gcExpiredWorker(ctx, masterClient, config, cancel, done)
	}
	for i := 0; i < config.NumExpireRelWorkers; i++ {
		go expireRelationsWorker(ctx, masterClient, config, cancel, done)
	}

	log.Printf("Stress test running. Press Ctrl+C to stop.")

	// Wait for interrupt or context cancellation
	select {
	case <-sigChan:
		log.Printf("Shutting down...")
		cancel()
		close(done)
		time.Sleep(1 * time.Second)
		log.Printf("Test driver stopped. Final BUSY error total: %d", busyErrorCount.Load())
	case <-ctx.Done():
		log.Printf("Shutting down due to master connectivity loss...")
		close(done)
		time.Sleep(1 * time.Second)
		log.Printf("Test driver stopped. Final BUSY error total: %d", busyErrorCount.Load())
	}
}
