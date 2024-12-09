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

	// Create a new Redis client with auto-pipelining disabled
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:           []string{"vk.0uiite.ng.0001.use1.cache.amazonaws.com:6379"}, // Replace with your Redis server address
		DisableAutoPipelining: true, // Disable auto-pipelining
	})
	if err != nil {
		return // Silently return on error
	}
	defer client.Close()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Start timer
	start := time.Now()

	// Write keys individually
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := fmt.Sprintf("value:%d", rand.Int())
		
		cmd := client.B().Set().Key(key).Value(value).Build()
		_ = client.Do(context.Background(), cmd).Error() // Ignore errors
	}

	// Calculate the elapsed time
	elapsed := time.Since(start)

	// Print completion message
	fmt.Printf("Operation complete: Wrote %d random keys in %s\n", totalKeys, elapsed)
}
