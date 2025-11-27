package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	MasterAddr             string
	ReplicaAddr            string
	Replica2Addr           string   // Optional second replica address
	QueryReplicas          []string // List of replica addresses to query (empty = only master)
	NumGraphs              int
	TargetNodesPerGraph    int // Number of nodes to create per graph
	NumUpdateWorkers       int // Number of concurrent update workers
	NumDynamicGraphWorkers int // Number of dynamic graph create/delete workers
	NumQueryWorkers        int // Number of concurrent query workers
	NumGCGarbageWorkers    int // Number of concurrent GC garbage workers (delete/recreate nodes)
	JitterMaxMs            int // Maximum jitter in milliseconds (0-1000)
	UpdateInterval         time.Duration
	DynamicGraphInterval   time.Duration // Interval for dynamic graph operations
	QueryInterval          time.Duration // Interval for query operations
	GCGarbageInterval      time.Duration // Interval for GC garbage operations
	BGSAVEInterval         time.Duration
	SimpleSeedOnly         bool // If true, only create one node per graph (for testing)
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
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			numUpdateWorkers = parsed
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

	return Config{
		MasterAddr:             master,
		ReplicaAddr:            replica,
		Replica2Addr:           replica2,
		QueryReplicas:          queryReplicas,
		NumGraphs:              numGraphs,
		TargetNodesPerGraph:    targetNodesPerGraph,
		NumUpdateWorkers:       numUpdateWorkers,
		NumDynamicGraphWorkers: numDynamicGraphWorkers,
		NumQueryWorkers:        numQueryWorkers,
		NumGCGarbageWorkers:    numGCGarbageWorkers,
		JitterMaxMs:            jitterMaxMs,
		UpdateInterval:         updateInterval,
		DynamicGraphInterval:   dynamicGraphInterval,
		QueryInterval:          queryInterval,
		GCGarbageInterval:      gcGarbageInterval,
		BGSAVEInterval:         bgsaveInterval,
		SimpleSeedOnly:         simpleSeedOnly,
	}
}

func createRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
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

func populateGraph(ctx context.Context, client *redis.Client, graphName string, targetNodes int) error {
	log.Printf("Populating graph %s (target: %d nodes)", graphName, targetNodes)

	// Step 1: Seed with one initial node
	now := time.Now().Unix()
	seedQuery := fmt.Sprintf("CREATE (n:Node {name: 'node-00', _updated: %d})", now)
	result, err := client.Do(ctx, "GRAPH.QUERY", graphName, seedQuery).Result()
	if err != nil {
		return fmt.Errorf("failed to seed initial node: %w", err)
	}
	log.Printf("Seed query for %s completed, result type: %T", graphName, result)

	// Verify seed node was created
	time.Sleep(100 * time.Millisecond)
	seedCount, err := getNodeCount(ctx, client, graphName)
	if err != nil {
		log.Printf("Warning: Could not verify seed node count for %s: %v", graphName, err)
	} else {
		log.Printf("After seed, %s has %d nodes", graphName, seedCount)
	}

	// Step 2: Iteratively grow the graph
	// In each iteration: connect all existing nodes, then add one new node
	for i := 1; i < targetNodes; i++ {
		// First, create relationships between all existing nodes
		quadraticQuery := "MATCH (a), (b) CREATE (a)-[:REL]->(b)"
		_, err := client.Do(ctx, "GRAPH.QUERY", graphName, quadraticQuery).Result()
		if err != nil {
			log.Printf("Warning: quadratic query failed for %s (iteration %d): %v", graphName, i, err)
			// Continue anyway
		}

		// Then, add one new node with name and timestamp
		nodeName := fmt.Sprintf("node-%02d", i)
		now := time.Now().Unix()
		createNodeQuery := fmt.Sprintf("CREATE (n:Node {name: '%s', _updated: %d})", nodeName, now)
		result, err = client.Do(ctx, "GRAPH.QUERY", graphName, createNodeQuery).Result()
		if err != nil {
			log.Printf("Warning: Failed to create node %s in %s: %v", nodeName, graphName, err)
			// Continue anyway
		} else if i%10 == 0 {
			// Log progress every 10 nodes
			log.Printf("Created node %s in %s (progress: %d/%d)", nodeName, graphName, i+1, targetNodes)
		}

		// Small delay between iterations
		time.Sleep(50 * time.Millisecond)
	}

	// Verify final node count
	time.Sleep(200 * time.Millisecond)
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
	seedQuery := fmt.Sprintf("CREATE (n:Node {name: 'node-00', _updated: %d})", now)

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
			log.Printf("Error checking graph %s: %v", graphName, err)
			continue
		}

		if !exists {
			if config.SimpleSeedOnly {
				// Simple seed: just create one node
				if err := simpleSeedGraph(ctx, masterClient, graphName); err != nil {
					log.Printf("Failed to seed graph %s: %v", graphName, err)
					return fmt.Errorf("failed to seed graph %s: %w", graphName, err)
				}
			} else {
				// Full population
				log.Printf("Graph %s does not exist, populating...", graphName)
				if err := populateGraph(ctx, masterClient, graphName, config.TargetNodesPerGraph); err != nil {
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
						if err := populateGraph(ctx, masterClient, graphName, config.TargetNodesPerGraph); err != nil {
							log.Printf("Failed to repopulate graph %s: %v", graphName, err)
						}
					} else {
						log.Printf("Graph %s already populated (%d nodes)", graphName, count)
					}
				}
			}
		}
	}

	// Also populate dynamic graphs
	log.Printf("Checking %d dynamic graphs for population...", config.NumGraphs)
	for i := 0; i < config.NumGraphs; i++ {
		graphName := fmt.Sprintf("dynamic-graph-%d", i)

		exists, err := graphExists(ctx, masterClient, graphName)
		if err != nil {
			log.Printf("Error checking dynamic graph %s: %v", graphName, err)
			continue
		}

		if !exists {
			if config.SimpleSeedOnly {
				// Simple seed: just create one node
				if err := simpleSeedGraph(ctx, masterClient, graphName); err != nil {
					log.Printf("Failed to seed dynamic graph %s: %v", graphName, err)
					// Don't fail completely, just log and continue
				}
			} else {
				// Full population
				log.Printf("Dynamic graph %s does not exist, populating...", graphName)
				if err := populateGraph(ctx, masterClient, graphName, config.TargetNodesPerGraph); err != nil {
					log.Printf("Failed to populate dynamic graph %s: %v", graphName, err)
					// Don't fail completely, just log and continue
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
						log.Printf("Dynamic graph %s exists but has 0 nodes, reseeding...", graphName)
						if err := simpleSeedGraph(ctx, masterClient, graphName); err != nil {
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
						if err := populateGraph(ctx, masterClient, graphName, config.TargetNodesPerGraph); err != nil {
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
	for i := 0; i < config.NumGraphs; i++ {
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

func updateWorker(ctx context.Context, client *redis.Client, config Config, done chan struct{}) {
	ticker := time.NewTicker(config.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			// Add random jitter to desynchronize workers
			time.Sleep(randomJitter(config.JitterMaxMs))

			// Pick a random graph
			graphNum := rand.Intn(config.NumGraphs)
			graphName := fmt.Sprintf("graph-%d", graphNum)

			// Randomly decide whether to update all nodes or only a subset (~50%)
			// This adds variability to the workload and lock contention patterns
			updateAll := rand.Float32() < 0.5
			now := time.Now().Unix()

			var updateNodesQuery string
			if updateAll {
				// Update all nodes
				updateNodesQuery = fmt.Sprintf("MATCH (n) SET n._updated = %d", now)
			} else {
				// Update roughly 50% of nodes using modulo on node ID
				// This creates a different query pattern and lock contention
				modValue := rand.Intn(2) // Randomly choose 0 or 1 for modulo
				updateNodesQuery = fmt.Sprintf("MATCH (n) WHERE id(n) %% 2 = %d SET n._updated = %d", modValue, now)
			}

			_, err := client.Do(ctx, "GRAPH.QUERY", graphName, updateNodesQuery).Result()
			if err != nil {
				log.Printf("Update nodes failed for %s: %v", graphName, err)
				continue
			}

			// Then, update relationships (if any exist)
			// Also vary relationship updates - sometimes update all, sometimes subset
			if rand.Float32() < 0.5 {
				// Update all relationships
				updateRelsQuery := fmt.Sprintf("MATCH (n)-[r]->(m) SET r._updated = %d", now)
				_, err2 := client.Do(ctx, "GRAPH.QUERY", graphName, updateRelsQuery).Result()
				if err2 != nil {
					// It's okay if there are no relationships, just log it
					log.Printf("Update relationships failed for %s (may have no relationships): %v", graphName, err2)
				}
			} else {
				// Update relationships only for nodes with even IDs (roughly 50%)
				updateRelsQuery := fmt.Sprintf("MATCH (n)-[r]->(m) WHERE id(n) %% 2 = 0 SET r._updated = %d", now)
				_, err2 := client.Do(ctx, "GRAPH.QUERY", graphName, updateRelsQuery).Result()
				if err2 != nil {
					// It's okay if there are no relationships, just log it
					log.Printf("Update relationships failed for %s (may have no relationships): %v", graphName, err2)
				}
			}

			updateType := "all"
			if !updateAll {
				updateType = "subset"
			}
			log.Printf("Updated %s nodes and relationships in %s", updateType, graphName)
		}
	}
}

func dynamicGraphWorker(ctx context.Context, client *redis.Client, config Config, done chan struct{}) {
	ticker := time.NewTicker(config.DynamicGraphInterval)
	defer ticker.Stop()

	// Use a small subset of dynamic graphs to avoid uncontrolled growth
	// Cycle through NUM_GRAPHS dynamic graphs
	maxDynamicGraphs := config.NumGraphs

	for {
		select {
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

			// Delete the graph if it exists (this triggers Globals_RemoveGraphByName)
			// Use DEL command to delete the graph key
			// If the graph doesn't exist (e.g., deleted by another worker), that's fine - just continue
			deleted, err := client.Del(ctx, graphName).Result()
			if err != nil {
				// Check if it's a "graph doesn't exist" error (expected when another worker deleted it)
				errStr := err.Error()
				if strings.Contains(errStr, "not found") ||
					strings.Contains(errStr, "does not exist") ||
					strings.Contains(errStr, "Unknown graph") ||
					strings.Contains(errStr, "empty key") {
					// Graph doesn't exist - this is expected, just continue to population
					log.Printf("Graph %s doesn't exist (may have been deleted by another worker), proceeding to populate", graphName)
				} else {
					// Unexpected error
					log.Printf("Warning: Failed to delete %s: %v", graphName, err)
				}
			} else if deleted > 0 {
				log.Printf("Deleted %s", graphName)
			} else {
				// DEL returned 0, meaning the key didn't exist (another worker may have deleted it)
				log.Printf("Graph %s doesn't exist (already deleted), proceeding to populate", graphName)
			}

			// Small delay before recreating
			time.Sleep(50 * time.Millisecond)

			// Recreate and populate the graph (this triggers Globals_AddGraph)
			if config.SimpleSeedOnly {
				if err := simpleSeedGraph(ctx, client, graphName); err != nil {
					// Check if it's a transient error (graph might have been recreated by another worker)
					errStr := err.Error()
					if strings.Contains(errStr, "already exists") {
						log.Printf("Graph %s already exists (created by another worker), skipping", graphName)
					} else {
						log.Printf("Failed to recreate %s (simple seed): %v", graphName, err)
					}
				} else {
					log.Printf("Recreated %s (simple seed)", graphName)
				}
			} else {
				if err := populateGraph(ctx, client, graphName, config.TargetNodesPerGraph); err != nil {
					// Check if it's a transient error (graph might have been recreated by another worker)
					errStr := err.Error()
					if strings.Contains(errStr, "already exists") {
						log.Printf("Graph %s already exists (created by another worker), skipping", graphName)
					} else {
						log.Printf("Failed to recreate %s: %v", graphName, err)
					}
				} else {
					log.Printf("Recreated %s with %d nodes", graphName, config.TargetNodesPerGraph)
				}
			}
		}
	}
}

func queryWorker(ctx context.Context, masterClient *redis.Client, replicaClients []*redis.Client, replicaAddrs []string, allowedReplicas []string, config Config, workerID int, done chan struct{}) {
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

	// Predefined graph-walking queries that traverse relationships
	// More intensive than before but still bounded to avoid timeouts
	// Avoiding multi-hop paths to prevent exponential complexity
	queries := []string{
		"MATCH (n)-[r]->(m) WHERE n._updated > 0 AND m._updated > 0 RETURN count(r) LIMIT 100",         // Multi-condition filter
		"MATCH (n) WHERE n._updated > 0 RETURN count(n)",                                               // Filtered node count
		"MATCH (n)-[r]->(m) WHERE id(n) < id(m) RETURN count(r) LIMIT 100",                             // ID-based filtering
		"MATCH (n)-[r]->(m) WHERE n._updated > 0 RETURN n, m, r ORDER BY n._updated LIMIT 50",          // Return with sorting
		"MATCH (n)-[r]->(m) WHERE n.name IS NOT NULL AND m.name IS NOT NULL RETURN count(r) LIMIT 200", // Property existence checks
		"MATCH (n) WHERE n._updated > 0 RETURN n.name, n._updated ORDER BY n._updated DESC LIMIT 100",  // Property access with sorting
		"MATCH (n)-[r]->(m) WHERE id(n) % 2 = 0 AND id(m) % 2 = 0 RETURN count(r) LIMIT 200",           // Multiple modulo filters
		"MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 50",                                                   // Return full patterns (more data transfer)
		"MATCH (n) WHERE n._updated > 0 RETURN collect(n.name) LIMIT 1",                                // Aggregation with collection
		"MATCH (n)-[r]->(m) WHERE n._updated > 0 RETURN distinct n.name LIMIT 100",                     // Distinct with property access
	}

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			// Add random jitter to desynchronize workers
			time.Sleep(randomJitter(config.JitterMaxMs))

			// Randomly choose from allowed clients (master + allowed replicas)
			selected := allowedClients[rand.Intn(len(allowedClients))]
			client := selected.client
			clientName := selected.name

			// Randomly choose between regular graphs and dynamic graphs
			var graphName string
			if rand.Float32() < 0.5 {
				// Regular graph
				graphNum := rand.Intn(config.NumGraphs)
				graphName = fmt.Sprintf("graph-%d", graphNum)
			} else {
				// Dynamic graph
				graphNum := rand.Intn(config.NumGraphs)
				graphName = fmt.Sprintf("dynamic-graph-%d", graphNum)
			}

			// Randomly select a query from our predefined set
			queryIdx := rand.Intn(len(queries))
			query := queries[queryIdx]

			// Execute the query using GRAPH.RO_QUERY (read-only, works on both master and replica)
			startTime := time.Now()
			_, err := client.Do(ctx, "GRAPH.RO_QUERY", graphName, query).Result()
			duration := time.Since(startTime)

			if err != nil {
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
				log.Printf("Query worker %d: Query[%d] failed on %s for %s (took %v): %v", workerID, queryIdx, clientName, graphName, duration, err)
			} else {
				log.Printf("Query worker %d: Query[%d] executed on %s for %s (took %v)", workerID, queryIdx, clientName, graphName, duration)
			}
		}
	}
}

func gcGarbageWorker(ctx context.Context, client *redis.Client, config Config, workerID int, done chan struct{}) {
	ticker := time.NewTicker(config.GCGarbageInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			// Add random jitter to desynchronize workers
			time.Sleep(randomJitter(config.JitterMaxMs))

			// Pick a random graph (regular graphs only, to avoid conflicts with dynamic graph worker)
			graphNum := rand.Intn(config.NumGraphs)
			graphName := fmt.Sprintf("graph-%d", graphNum)

			// Delete 25-50 nodes to create garbage for RediSearch GC
			// Each node has 2 indexes (_updated and name), so deleting 1 node = 2 deleted docs
			// To hit the 100-doc threshold, we need ~50 nodes deleted (creates 100 deleted docs)
			// Use WITH ... LIMIT ... DELETE pattern (LIMIT must come before DELETE in Cypher)
			deleteCount := 25 + rand.Intn(26) // Random between 25-50 nodes (creates 50-100 deleted docs)
			deleteQuery := fmt.Sprintf("MATCH (n) WITH n LIMIT %d DELETE n", deleteCount)
			_, err := client.Do(ctx, "GRAPH.QUERY", graphName, deleteQuery).Result()
			if err != nil {
				// Graph might not exist or have nodes, that's okay
				errStr := err.Error()
				if strings.Contains(errStr, "not found") ||
					strings.Contains(errStr, "does not exist") ||
					strings.Contains(errStr, "Unknown graph") ||
					strings.Contains(errStr, "empty key") {
					// Graph doesn't exist, skip silently
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
					// Log but continue - some failures are expected
					log.Printf("GC garbage worker %d: Failed to recreate node in %s: %v", workerID, graphName, err)
					break
				}
			}

			log.Printf("GC garbage worker %d: Deleted and recreated nodes in %s (creates garbage for RediSearch GC)", workerID, graphName)
		}
	}
}

func bgsaveWorker(ctx context.Context, masterClient *redis.Client, replicaClients []*redis.Client, config Config, done chan struct{}) {
	ticker := time.NewTicker(config.BGSAVEInterval)
	defer ticker.Stop()

	// Track consecutive blocked BGSAVE attempts per instance
	// Only log 🚨 BLOCKED after 10 consecutive failures
	BLOCKED_THRESHOLD := 10
	blockedCounters := make(map[string]int)

	// Helper function to handle BGSAVE result for an instance
	handleBGSaveResult := func(instanceName string, result interface{}, err error) {
		if err != nil {
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
		case <-done:
			return
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
	log.Printf("Config: Master=%s, Replicas=%s, QueryReplicas=%s, Graphs=%d, NodesPerGraph=%d, UpdateWorkers=%d, DynamicGraphWorkers=%d, QueryWorkers=%d, GCGarbageWorkers=%d, JitterMaxMs=%d, UpdateInterval=%v, DynamicGraphInterval=%v, QueryInterval=%v, GCGarbageInterval=%v, BGSAVEInterval=%v",
		config.MasterAddr, replicaInfo, queryReplicasInfo, config.NumGraphs, config.TargetNodesPerGraph, config.NumUpdateWorkers, config.NumDynamicGraphWorkers, config.NumQueryWorkers, config.NumGCGarbageWorkers, config.JitterMaxMs, config.UpdateInterval, config.DynamicGraphInterval, config.QueryInterval, config.GCGarbageInterval, config.BGSAVEInterval)

	ctx := context.Background()

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

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start workers
	done := make(chan struct{})
	// Start multiple update workers for concurrent updates
	for i := 0; i < config.NumUpdateWorkers; i++ {
		go updateWorker(ctx, masterClient, config, done)
	}
	// Start dynamic graph workers for create/delete operations
	for i := 0; i < config.NumDynamicGraphWorkers; i++ {
		go dynamicGraphWorker(ctx, masterClient, config, done)
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
		go queryWorker(ctx, masterClient, replicaClients, replicaAddrs, config.QueryReplicas, config, i, done)
	}
	// Start GC garbage workers to create deleted documents for RediSearch GC
	for i := 0; i < config.NumGCGarbageWorkers; i++ {
		go gcGarbageWorker(ctx, masterClient, config, i, done)
	}
	go bgsaveWorker(ctx, masterClient, replicaClients, config, done)

	log.Printf("Stress test running. Press Ctrl+C to stop.")

	// Wait for interrupt
	<-sigChan
	log.Printf("Shutting down...")
	close(done)
	time.Sleep(1 * time.Second)
	log.Printf("Test driver stopped")
}
