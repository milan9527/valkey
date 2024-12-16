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
BIG_KEY_SIZE = 1000000  # 1MB
KEYS_PER_CYCLE = 100

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

def create_and_delete_big_keys(r):
    print(f"Creating and deleting {KEYS_PER_CYCLE} big keys...")
    for i in range(KEYS_PER_CYCLE):
        key = f"{KEY_PREFIX}big_{i}"
        r.set(key, generate_random_string(BIG_KEY_SIZE))
        
        # Delete immediately to create large memory gaps
        r.delete(key)

def fragment_memory(r):
    for cycle in range(CYCLE_COUNT):
        print(f"Starting fragmentation cycle {cycle + 1}")
        
        create_and_delete_big_keys(r)
        
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
