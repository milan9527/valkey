import asyncio
import time
from glide import GlideClusterClientConfiguration, NodeAddress, GlideClusterClient, ReadFrom

async def connect_to_cluster():
    # Replace this with your actual Valkey cluster address
    addresses = [NodeAddress("redis-test.0uiite.clustercfg.use1.cache.amazonaws.com", 6379)]
    config = GlideClusterClientConfiguration(addresses)
    
    # Set read preference to prefer replicas
    config.read_from = ReadFrom.PREFER_REPLICA
    
    return await GlideClusterClient.create(config)

async def write_keys(client):
    # Write some keys to the cluster
    for i in range(10):
        key = f"test_key_{i}"
        value = f"test_value_{i}_{int(time.time())}"
        set_result = await client.set(key, value)
        print(f"Wrote {key}: {value}, Result: {set_result}")

async def read_keys(client):
    # Read keys
    for i in range(10):
        key = f"test_key_{i}"
        get_result = await client.get(key)
        print(f"Read {key}: {get_result}")

async def main():
    try:
        # Connect to the Valkey cluster
        client = await connect_to_cluster()
        
        start_time = time.time()
        end_time = start_time + 600  # 10 minutes = 600 seconds

        while time.time() < end_time:
            # Write keys
            print("\nWriting keys...")
            await write_keys(client)
            
            # Wait a moment to ensure replication
            print("Waiting for replication...")
            await asyncio.sleep(2)
            
            # Read keys (preferentially from replicas)
            print("Reading keys (preferentially from replicas)...")
            await read_keys(client)
            
            # Wait for a short period before the next iteration
            await asyncio.sleep(5)

        print("\nFinished 10-minute run.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'client' in locals():
            await client.close()

if __name__ == "__main__":
    asyncio.run(main())
