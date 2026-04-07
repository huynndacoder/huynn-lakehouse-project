"""
Redis-based caching service for query result caching.

This module provides a simple caching layer to reduce database load
and improve response times for frequently accessed data.
"""

import redis
import json
import logging
import hashlib
from typing import Optional, Any, Callable
from functools import wraps
import os

logger = logging.getLogger(__name__)


class CacheService:
    """
    Redis-based cache for query results.

    Features:
    - Automatic serialization/deserialization (JSON)
    - Key generation with hashing
    - Graceful degradation if Redis unavailable
    - TTL-based expiration
    """

    def __init__(self, redis_url: str = None, default_ttl: int = 300):
        """
        Initialize Redis cache service.

        Args:
            redis_url: Redis connection URL (default from env)
            default_ttl: Default time-to-live in seconds (default: 5 minutes)
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
        self.default_ttl = default_ttl
        self.client = None
        self.enabled = os.getenv("REDIS_CACHE_ENABLED", "true").lower() == "true"

        if self.enabled:
            try:
                self.client = redis.from_url(self.redis_url, decode_responses=True)
                # Test connection
                self.client.ping()
                logger.info(f"Redis cache initialized (TTL: {default_ttl}s)")
            except Exception as e:
                logger.warning(f"Redis initialization failed, cache disabled: {e}")
                self.client = None
                self.enabled = False
        else:
            logger.info("Redis cache disabled by configuration")

    def _generate_key(self, prefix: str, **kwargs) -> str:
        """
        Generate a cache key from function arguments.

        Args:
            prefix: Key prefix (e.g., 'dashboard_stats')
            **kwargs: Key-value pairs to hash

        Returns:
            Cache key string
        """
        # Sort keys for consistent hashing
        key_data = json.dumps(kwargs, sort_keys=True, default=str)
        key_hash = hashlib.md5(key_data.encode()).hexdigest()[:12]
        return f"{prefix}:{key_hash}"

    def get(self, key: str) -> Optional[Any]:
        """
        Get cached value.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found
        """
        if not self.enabled or not self.client:
            return None

        try:
            value = self.client.get(key)
            if value:
                return json.loads(value)
        except Exception as e:
            logger.warning(f"Cache get failed for key {key}: {e}")

        return None

    def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """
        Set cached value.

        Args:
            key: Cache key
            value: Value to cache (must be JSON-serializable)
            ttl: Time-to-live in seconds (default from config)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            self.client.setex(
                key, ttl or self.default_ttl, json.dumps(value, default=str)
            )
            return True
        except Exception as e:
            logger.warning(f"Cache set failed for key {key}: {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Delete cached value.

        Args:
            key: Cache key

        Returns:
            True if deleted, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            self.client.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Cache delete failed for key {key}: {e}")
            return False

    def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching pattern.

        Args:
            pattern: Key pattern (e.g., 'dashboard:*')

        Returns:
            Number of keys deleted
        """
        if not self.enabled or not self.client:
            return 0

        try:
            keys = self.client.keys(pattern)
            if keys:
                deleted = self.client.delete(*keys)
                logger.debug(f"Deleted {deleted} cache keys matching {pattern}")
                return deleted
        except Exception as e:
            logger.warning(f"Cache delete pattern failed for {pattern}: {e}")

        return 0

    def health_check(self) -> bool:
        """
        Check if Redis is available.

        Returns:
            True if healthy, False otherwise
        """
        if not self.client:
            return False

        try:
            return self.client.ping()
        except:
            return False


# Global cache instance
_cache_service = None


def get_cache_service() -> Optional[CacheService]:
    """
    Get or create cache service singleton.

    Returns:
        CacheService instance or None if unavailable
    """
    global _cache_service

    if _cache_service is None:
        try:
            from config import settings

            _cache_service = CacheService(
                redis_url=settings.REDIS_URL, default_ttl=settings.REDIS_CACHE_TTL
            )
        except Exception as e:
            logger.warning(f"Failed to initialize cache service: {e}")
            return None

    return _cache_service


def cached(prefix: str, ttl: int = None):
    """
    Decorator to cache function results.

    Usage:
        @cached(prefix="dashboard_stats", ttl=300)
        def get_dashboard_stats(hours_back=24):
            # ... query database ...
            return stats

    Args:
        prefix: Cache key prefix
        ttl: Time-to-live in seconds (default from config)

    Returns:
        Decorated function with caching
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get cache service
            cache = get_cache_service()

            if not cache or not cache.enabled:
                # Cache unavailable, execute directly
                return func(*args, **kwargs)

            # Generate cache key
            # Include function name and arguments
            cache_key = cache._generate_key(
                prefix, func=func.__name__, args=str(args), **kwargs
            )

            # Try cache first
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache HIT: {cache_key}")
                return cached_result

            # Execute function
            logger.debug(f"Cache MISS: {cache_key}")
            result = func(*args, **kwargs)

            # Cache result
            if result is not None:
                cache.set(cache_key, result, ttl)

            return result

        return wrapper

    return decorator
