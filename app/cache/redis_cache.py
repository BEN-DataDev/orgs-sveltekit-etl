import redis
import json
from config.config import Config
from typing import Any, Optional

# class RedisCache:
#     def __init__(self):
#         self.redis_client = redis.from_url(Config.REDIS_URL)

#     def get(self, key):
#         value = self.redis_client.get(key)
#         return json.loads(value) if value else None

#     def set(self, key, value, expiration=Config.CACHE_EXPIRATION):
#         self.redis_client.setex(key, expiration, json.dumps(value))

#     def delete(self, key):
#         self.redis_client.delete(key)

# app/cache/redis_cache.py
import redis
import json


class RedisCache:
    def __init__(self, host: str = Config.REDIS_HOST, port: int = Config.REDIS_PORT, username: Optional[str] = Config.REDIS_USERNAME, password: Optional[str] = Config.REDIS_PASSWORD, db: int = Config.REDIS_DB):
        """
        Initialize Redis client with connection parameters.
        
        Args:
            host: Redis server hostname
            port: Redis server port
            password: Redis authentication password
            db: Redis database number
        """
        self.client = redis.Redis(
            host=host,
            port=port,
            username='default',
            password=password,
            db=db,
            decode_responses=True  # Automatically decode responses to strings
        )

        if not self.test_connection():
            raise redis.RedisError("Failed to connect to Redis")

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve data from cache by key.
        
        Args:
            key: Cache key to retrieve
        
        Returns:
            Cached data (deserialized JSON) or None if key does not exist
        """
        try:
            data = self.client.get(key)
            if data:
                return json.loads(data)  # Deserialize JSON string
            return None
        except redis.RedisError as e:
            print(f"Redis get error: {e}")
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Store data in cache.
        
        Args:
            key: Cache key
            value: Data to cache (will be serialized to JSON)
            ex: Expiry time in seconds (optional)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Serialize value to JSON string
            serialized_value = json.dumps(value)
            return self.client.set(key, serialized_value, ex=ttl)
        except redis.RedisError as e:
            print(f"Redis set error: {e}")
            return False

    def test_connection(self) -> bool:
        """
        Test Redis connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            return self.client.ping()
        except redis.RedisError as e:
            print(f"Redis connection error: {e}")
            return False