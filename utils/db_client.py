import psycopg2
import psycopg2.pool
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import threading

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
        self._lock = threading.Lock()
    
    def connect(self):
        """Initialize connection pool"""
        with self._lock:
            if self.connection_pool is not None:
                return
                
            try:
                self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=5,
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    connect_timeout=10
                )
                logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}")
                raise
    
    def disconnect(self):
        """Close connection pool"""
        with self._lock:
            if self.connection_pool:
                self.connection_pool.closeall()
                self.connection_pool = None
                logger.info("Disconnected from PostgreSQL")
    
    def is_connected(self) -> bool:
        """Check if connection pool is active and healthy"""
        if not self.connection_pool:
            return False
        try:
            conn = self.connection_pool.getconn()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    conn.commit()
                self.connection_pool.putconn(conn)
                return True
        except Exception as e:
            logger.error(f"Connection health check failed: {e}")
            return False
    
    def ensure_connected(self):
        """Ensure connection pool is active, reconnect if needed"""
        if not self.is_connected():
            logger.info("Reconnecting to database...")
            self.disconnect()  # Clean up any stale connections
            self.connect()
    
    def get_recent_simulations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent simulation runs from database"""
        self.ensure_connected()
        
        conn = None
        try:
            conn = self.connection_pool.getconn()
            with conn.cursor() as cur:
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
                    LIMIT %s
                """
                
                cur.execute(query, (limit,))
                rows = cur.fetchall()
                
                # Get column names
                columns = [desc[0] for desc in cur.description]
                
                # Convert rows to dictionaries
                simulations = []
                for row in rows:
                    sim = dict(zip(columns, row))
                    # Convert datetime objects to ISO strings for JSON serialization
                    for key, value in sim.items():
                        if isinstance(value, datetime):
                            sim[key] = value.isoformat()
                    simulations.append(sim)
                
                return simulations
                
        except Exception as e:
            logger.error(f"Failed to get recent simulations: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def get_simulation_by_id(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific simulation by run_id"""
        self.ensure_connected()
        
        conn = None
        try:
            conn = self.connection_pool.getconn()
            with conn.cursor() as cur:
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
                    WHERE run_id = %s
                """
                
                cur.execute(query, (run_id,))
                row = cur.fetchone()
                
                if row:
                    # Get column names
                    columns = [desc[0] for desc in cur.description]
                    sim = dict(zip(columns, row))
                    
                    # Convert datetime objects to ISO strings
                    for key, value in sim.items():
                        if isinstance(value, datetime):
                            sim[key] = value.isoformat()
                    return sim
                
                return None
                
        except Exception as e:
            logger.error(f"Failed to get simulation {run_id}: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    def health_check(self) -> bool:
        """Check database connection health"""
        try:
            self.ensure_connected()
            
            conn = self.connection_pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    conn.commit()
                return True
            finally:
                self.connection_pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

# Global client instance
_db_client = None
_client_lock = threading.Lock()

def get_db_client() -> DatabaseClient:
    """Get or create database client instance"""
    global _db_client
    with _client_lock:
        if _db_client is None:
            _db_client = DatabaseClient()
            _db_client.connect()
        return _db_client

def cleanup_db_client():
    """Clean up database client and close connections"""
    global _db_client
    with _client_lock:
        if _db_client and _db_client.connection_pool:
            _db_client.disconnect()
            _db_client = None

def get_recent_simulations(limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent simulation runs from database with error recovery"""
    try:
        client = get_db_client()
        return client.get_recent_simulations(limit)
    except Exception as e:
        logger.error(f"Failed to get recent simulations: {e}")
        # Try one more time with a fresh connection
        try:
            cleanup_db_client()
            client = get_db_client()
            return client.get_recent_simulations(limit)
        except Exception as retry_e:
            logger.error(f"Retry failed: {retry_e}")
            raise

def get_simulation_by_id(run_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific simulation by run_id with error recovery"""
    try:
        client = get_db_client()
        return client.get_simulation_by_id(run_id)
    except Exception as e:
        logger.error(f"Failed to get simulation {run_id}: {e}")
        # Try one more time with a fresh connection
        try:
            cleanup_db_client()
            client = get_db_client()
            return client.get_simulation_by_id(run_id)
        except Exception as retry_e:
            logger.error(f"Retry failed: {retry_e}")
            raise

def check_db_health() -> bool:
    """Check database connection health with error recovery"""
    try:
        client = get_db_client()
        return client.health_check()
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        # Try one more time with a fresh connection
        try:
            cleanup_db_client()
            client = get_db_client()
            return client.health_check()
        except Exception as retry_e:
            logger.error(f"Health check retry failed: {retry_e}")
            return False

# Keep legacy function names for backward compatibility
get_recent_simulations_sync = get_recent_simulations
get_simulation_by_id_sync = get_simulation_by_id
check_db_health_sync = check_db_health