import redis
import random
import string
import time

# Redis connection details
REDIS_HOST = 'vk8.0uiite.ng.0001.use1.cache.amazonaws.com'
REDIS_PORT = 6379
#REDIS_PASSWORD = 'your_redis_password'

# Configuration
KEY_PREFIX = 'frag_test:'
CYCLE_COUNT = 10  # Number of fragmentation cycles

def connect_to_redis():
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            #password=REDIS_PASSWORD,
            decode_responses=True
        )
        r.ping()
        print("Successfully connected to Redis")
        return r
    except redis.ConnectionError:
        print("Failed to connect to Redis")
        return None

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_varied_size_keys(r, count):
    print(f"Creating {count} keys with varied sizes...")
    for i in range(count):
        key = f"{KEY_PREFIX}var_{i}"
        size = random.randint(1, 1000)
        r.set(key, generate_random_string(size))

def create_and_delete_large_keys(r, count):
    print(f"Creating and deleting {count} large keys...")
    for i in range(count):
        key = f"{KEY_PREFIX}large_{i}"
        r.set(key, generate_random_string(1000000))  # 1MB
        r.delete(key)

def create_and_trim_lists(r, count):
    print(f"Creating and trimming {count} lists...")
    for i in range(count):
        key = f"{KEY_PREFIX}list_{i}"
        r.rpush(key, *[generate_random_string(100) for _ in range(1000)])
        r.ltrim(key, 0, 499)  # Keep only first 500 elements

def create_and_modify_hashes(r, count):
    print(f"Creating and modifying {count} hashes...")
    for i in range(count):
        key = f"{KEY_PREFIX}hash_{i}"
        r.hmset(key, {f'field_{j}': generate_random_string(100) for j in range(100)})
        for j in range(50):  # Delete and add fields
            r.hdel(key, f'field_{j}')
            r.hset(key, f'new_field_{j}', generate_random_string(200))

def fragment_memory(r):
    for cycle in range(CYCLE_COUNT):
        print(f"Starting fragmentation cycle {cycle + 1}")
        
        create_varied_size_keys(r, 10000)
        create_and_delete_large_keys(r, 100)
        create_and_trim_lists(r, 1000)
        create_and_modify_hashes(r, 1000)
        
        # Delete a portion of keys
        all_keys = r.keys(f"{KEY_PREFIX}*")
        keys_to_delete = random.sample(all_keys, int(len(all_keys) * 0.7))
        if keys_to_delete:
            r.delete(*keys_to_delete)
        
        print(f"Cycle {cycle + 1} completed")
        get_memory_info(r)
        time.sleep(1)  # Give Redis a moment to process

def get_memory_info(r):
    info = r.info('memory')
    print(f"Used memory: {info['used_memory_human']}")
    print(f"Peak memory: {info['used_memory_peak_human']}")
    print(f"Fragmentation ratio: {info['mem_fragmentation_ratio']}")

if __name__ == "__main__":
    redis_client = connect_to_redis()
    if redis_client:
        fragment_memory(redis_client)
        print("Final memory state:")
        get_memory_info(redis_client)

