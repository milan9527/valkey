package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/redis/rueidis"
)

const (
	totalKeys = 1000000
	batchSize = 1000
)

func main() {
	// Disable logging output
	log.SetOutput(nil)

	// Create a new Redis client
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"vk.0uiite.ng.0001.use1.cache.amazonaws.com:6379"}, // Replace with your Redis server address
	})
	if err != nil {
		return // Silently return on error
	}
	defer client.Close()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Start timer
	start := time.Now()

	// Write keys in batches
	for i := 0; i < totalKeys; i += batchSize {
		cmds := make([]rueidis.Completed, 0, batchSize)

		for j := 0; j < batchSize && i+j < totalKeys; j++ {
			key := fmt.Sprintf("key:%d", i+j)
			value := fmt.Sprintf("value:%d", rand.Int())
			cmds = append(cmds, client.B().Set().Key(key).Value(value).Build())
		}

		_ = client.DoMulti(context.Background(), cmds...) // Ignore errors
	}

	// Calculate the elapsed time
	elapsed := time.Since(start)

	// Print completion message
	fmt.Printf("Operation complete: Wrote %d random keys in %s\n", totalKeys, elapsed)
}
