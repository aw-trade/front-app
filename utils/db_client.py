import asyncpg
import asyncio
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DatabaseClient:
    def __init__(self):
        # Database connection parameters from docker-compose.databases.yml
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = os.getenv("POSTGRES_DB", "trading_results")
        self.user = os.getenv("POSTGRES_USER", "trading_user")
        self.password = os.getenv("POSTGRES_PASSWORD", "trading_pass")
        
        self.connection_pool = None
    
    async def connect(self):
        """Initialize connection pool"""
        try:
            self.connection_pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=1,
                max_size=5,
                command_timeout=60
            )
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    async def disconnect(self):
        """Close connection pool"""
        if self.connection_pool:
            await self.connection_pool.close()
            self.connection_pool = None
            logger.info("Disconnected from PostgreSQL")
    
    async def is_connected(self) -> bool:
        """Check if connection pool is active and healthy"""
        if not self.connection_pool:
            return False
        try:
            async with self.connection_pool.acquire() as conn:
                await conn.execute("SELECT 1")
                return True
        except Exception:
            return False
    
    async def ensure_connected(self):
        """Ensure connection pool is active, reconnect if needed"""
        if not await self.is_connected():
            logger.info("Reconnecting to database...")
            await self.disconnect()  # Clean up any stale connections
            await self.connect()
    
    async def get_recent_simulations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent simulation runs from database"""
        await self.ensure_connected()
        
        try:
            async with self.connection_pool.acquire() as conn:
                query = """
                    SELECT 
                        run_id,
                        start_time,
                        end_time,
                        duration_seconds,
                        algorithm_version,
                        status,
                        initial_capital,
                        final_capital,
                        net_pnl,
                        return_pct,
                        total_trades,
                        win_rate,
                        max_drawdown,
                        sharpe_ratio,
                        created_at,
                        updated_at
                    FROM simulation_runs 
                    ORDER BY start_time DESC 
                    LIMIT $1
                """
                
                rows = await conn.fetch(query, limit)
                
                # Convert rows to dictionaries
                simulations = []
                for row in rows:
                    sim = dict(row)
                    # Convert datetime objects to ISO strings for JSON serialization
                    for key, value in sim.items():
                        if isinstance(value, datetime):
                            sim[key] = value.isoformat()
                    simulations.append(sim)
                
                return simulations
                
        except Exception as e:
            logger.error(f"Failed to get recent simulations: {e}")
            raise
    
    async def get_simulation_by_id(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific simulation by run_id"""
        await self.ensure_connected()
        
        try:
            async with self.connection_pool.acquire() as conn:
                query = """
                    SELECT 
                        run_id,
                        start_time,
                        end_time,
                        duration_seconds,
                        algorithm_version,
                        status,
                        initial_capital,
                        final_capital,
                        net_pnl,
                        return_pct,
                        total_trades,
                        win_rate,
                        max_drawdown,
                        sharpe_ratio,
                        signals_received,
                        signals_executed,
                        execution_rate,
                        created_at,
                        updated_at
                    FROM simulation_runs 
                    WHERE run_id = $1
                """
                
                row = await conn.fetchrow(query, run_id)
                
                if row:
                    sim = dict(row)
                    # Convert datetime objects to ISO strings
                    for key, value in sim.items():
                        if isinstance(value, datetime):
                            sim[key] = value.isoformat()
                    return sim
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to get simulation {run_id}: {e}")
            raise
    
    async def health_check(self) -> bool:
        """Check database connection health"""
        try:
            await self.ensure_connected()
            
            async with self.connection_pool.acquire() as conn:
                await conn.execute("SELECT 1")
                return True
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

# Global client instance
_db_client = None

async def get_db_client() -> DatabaseClient:
    """Get or create database client instance"""
    global _db_client
    if _db_client is None:
        _db_client = DatabaseClient()
        await _db_client.connect()
    return _db_client

# Streamlit session-based connection management
def get_streamlit_db_client() -> DatabaseClient:
    """Get or create Streamlit session-based database client with proper caching"""
    try:
        import streamlit as st
        
        # Use Streamlit's cache_resource to manage the connection pool
        @st.cache_resource
        def create_db_client():
            """Create and cache database client for Streamlit session"""
            client = DatabaseClient()
            # Initialize connection synchronously for Streamlit
            run_async(client.connect())
            return client
        
        return create_db_client()
    except ImportError:
        # Fallback for non-Streamlit environments
        return run_async(get_db_client())
        
async def cleanup_db_client():
    """Clean up database client and close connections"""
    global _db_client
    if _db_client and _db_client.connection_pool:
        await _db_client.disconnect()
        _db_client = None

def run_async(coro):
    """Helper function to run async functions in sync context with proper event loop management"""
    import concurrent.futures
    
    try:
        # Check if we're already in an async context
        try:
            asyncio.get_running_loop()
            # We're in an async context, need to run in a separate thread
            def run_in_thread():
                # Create a new event loop in this thread
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(coro)
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                return future.result(timeout=30)  # Add timeout to prevent hanging
                
        except RuntimeError:
            # No event loop running, we can run directly
            return asyncio.run(coro)
            
    except Exception as e:
        logger.error(f"Error in run_async: {e}")
        raise

def get_recent_simulations_sync(limit: int = 10) -> List[Dict[str, Any]]:
    """Synchronous wrapper for getting recent simulations with error recovery"""
    def _get_simulations():
        client = get_streamlit_db_client()
        
        async def _async_get():
            await client.ensure_connected()
            return await client.get_recent_simulations(limit)
        
        return run_async(_async_get())
    
    try:
        return _get_simulations()
    except Exception as e:
        logger.error(f"Failed to get recent simulations: {e}")
        # Try one more time with a fresh connection
        try:
            run_async(cleanup_db_client())
            return _get_simulations()
        except Exception as retry_e:
            logger.error(f"Retry failed: {retry_e}")
            raise

def get_simulation_by_id_sync(run_id: str) -> Optional[Dict[str, Any]]:
    """Synchronous wrapper for getting simulation by ID with error recovery"""
    def _get_simulation():
        client = get_streamlit_db_client()
        
        async def _async_get():
            await client.ensure_connected()
            return await client.get_simulation_by_id(run_id)
        
        return run_async(_async_get())
    
    try:
        return _get_simulation()
    except Exception as e:
        logger.error(f"Failed to get simulation {run_id}: {e}")
        # Try one more time with a fresh connection
        try:
            run_async(cleanup_db_client())
            return _get_simulation()
        except Exception as retry_e:
            logger.error(f"Retry failed: {retry_e}")
            raise

def check_db_health_sync() -> bool:
    """Synchronous wrapper for database health check with error recovery"""
    def _health_check():
        client = get_streamlit_db_client()
        
        async def _async_check():
            await client.ensure_connected()
            return await client.health_check()
        
        return run_async(_async_check())
    
    try:
        return _health_check()
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        # Try one more time with a fresh connection
        try:
            run_async(cleanup_db_client())
            return _health_check()
        except Exception as retry_e:
            logger.error(f"Health check retry failed: {retry_e}")
            return False


