"""
Database service for connecting to Databricks SQL
"""

import os
import asyncio
import logging
import json
import time
from typing import List, Dict, Any, Optional
from databricks import sql
from databricks.sql.exc import ServerOperationError, RequestError, SessionAlreadyClosedError
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Simple in-memory cache (can be replaced with Redis in production)
_cache = {}
_cache_timestamps = {}

class DatabaseService:
    """Service for handling Databricks SQL connections and queries"""

    def __init__(self, warehouse_id: str, hostname: str = None):
        from backend import config
        self.warehouse_id = warehouse_id
        self.hostname = hostname or config.DATABRICKS_HOSTNAME
        self._credentials_provider = config.get_sql_credentials_provider()
        # PAT fallback for local dev when no SP credentials
        self._access_token = config.DATABRICKS_TOKEN if self._credentials_provider is None else None
        self._connection = None
        self._last_connection_time = None
        self._connection_timeout = 300  # 5 minutes
        self._max_retries = 3

        if self._credentials_provider:
            logger.info("DatabaseService: using OAuth M2M (service principal)")
        elif self._access_token:
            logger.info("DatabaseService: using PAT token fallback")
        else:
            logger.warning("DatabaseService: no credentials available")

    def _create_connection(self):
        """Create a new SQL connection (sync, run in executor)."""
        connect_kwargs = dict(
            server_hostname=self.hostname,
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
        )
        if self._credentials_provider:
            connect_kwargs["credentials_provider"] = self._credentials_provider
        else:
            connect_kwargs["access_token"] = self._access_token or "dummy_token"
        return sql.connect(**connect_kwargs)

    async def get_connection(self, force_refresh=False):
        """Get or create a database connection with automatic refresh"""
        # Check if we need to refresh the connection
        should_refresh = (
            force_refresh or
            not self._connection or
            (self._last_connection_time and
             time.time() - self._last_connection_time > self._connection_timeout)
        )

        if should_refresh:
            await self._close_connection()
            try:
                # Run connection creation in thread pool since databricks.sql is sync
                loop = asyncio.get_event_loop()
                self._connection = await loop.run_in_executor(
                    None, self._create_connection
                )
                self._last_connection_time = time.time()
                logger.info("Database connection established/refreshed")
            except Exception as e:
                logger.error(f"Failed to create database connection: {e}")
                self._connection = None
                raise
        return self._connection

    async def _close_connection(self):
        """Safely close existing connection"""
        if self._connection:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._connection.close)
            except Exception as e:
                logger.debug(f"Error closing old connection (may be already closed): {e}")
            finally:
                self._connection = None

    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None, cache_ttl: Optional[int] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as list of dictionaries with retry logic"""
        # Check cache if TTL is specified
        if cache_ttl:
            cache_key = self._get_cache_key(query, params)
            cached_result = self._get_cached_result(cache_key, cache_ttl)
            if cached_result is not None:
                logger.info(f"Cache hit for query: {query[:50]}...")
                return cached_result

        last_error = None
        for attempt in range(self._max_retries):
            try:
                # Get connection (will refresh if needed)
                connection = await self.get_connection()

                # Execute query in thread pool
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    self._execute_sync_query,
                    connection,
                    query,
                    params
                )

                # Cache result if TTL is specified
                if cache_ttl:
                    self._cache_result(cache_key, result)
                    logger.info(f"Cached query result for {cache_ttl} seconds")

                return result

            except (RequestError, ServerOperationError, SessionAlreadyClosedError) as e:
                last_error = e
                logger.warning(f"Query attempt {attempt + 1}/{self._max_retries} failed: {type(e).__name__}: {str(e)[:100]}")

                # Force connection refresh on connection errors
                if attempt < self._max_retries - 1:
                    logger.info(f"Refreshing connection and retrying...")
                    await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                    # Force refresh the connection
                    await self.get_connection(force_refresh=True)

            except Exception as e:
                logger.error(f"Query execution failed with unexpected error: {e}")
                logger.error(f"Query: {query[:200]}...")
                logger.error(f"Params: {params}")
                raise Exception(f"Database query failed: {str(e)}")

        # If we exhausted all retries
        logger.error(f"Query failed after {self._max_retries} attempts")
        logger.error(f"Query: {query[:200]}...")
        logger.error(f"Params: {params}")
        raise Exception(f"Database query failed after {self._max_retries} retries: {str(last_error)}")

    def _execute_sync_query(self, connection, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Synchronous query execution (run in thread pool)"""
        with connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Get column names
            columns = [col[0] for col in cursor.description] if cursor.description else []

            # Fetch all rows and convert to list of dicts
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def _get_cache_key(self, query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """Generate cache key from query and parameters"""
        key_data = {"query": query, "params": params or {}}
        return str(hash(json.dumps(key_data, sort_keys=True)))

    def _get_cached_result(self, cache_key: str, ttl: int) -> Optional[List[Dict[str, Any]]]:
        """Get cached result if still valid"""
        if cache_key in _cache and cache_key in _cache_timestamps:
            cached_time = _cache_timestamps[cache_key]
            if datetime.now() - cached_time < timedelta(seconds=ttl):
                return _cache[cache_key]
        return None

    def _cache_result(self, cache_key: str, result: List[Dict[str, Any]]):
        """Cache query result with timestamp"""
        _cache[cache_key] = result
        _cache_timestamps[cache_key] = datetime.now()

    async def test_connection(self) -> bool:
        """Test database connection"""
        try:
            result = await self.execute_query("SELECT 1 as test")
            return len(result) == 1 and result[0]["test"] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    async def close(self):
        """Close database connection"""
        await self._close_connection()
        logger.info("Database connection closed")