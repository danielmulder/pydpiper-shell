# src/pydpiper_shell/core/managers/database_manager.py
import logging
import sqlite3
import threading
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Optional, Any

import pandas as pd

from pydpiper_shell.core.utils.path_utils import PathUtils
from pydpiper_shell.database_schema import DEFAULT_SCHEMA_SCRIPT

logger = logging.getLogger(__name__)

# Thread-local storage to ensure SQLite connections are not shared across threads
thread_local_storage = threading.local()


class DatabaseManager:
    """
    A 'dumb' Database Manager.

    Responsibility:
        - Handles SQLite connection lifecycles (opening, closing, caching per thread).
        - Executes raw SQL queries and scripts.
        - Manages database schema initialization via a constant.

    Constraints:
        - It does NOT contain business logic or complex data transformations.
        - It acts as a low-level data access layer.
    """

    def __init__(self, base_dir: Optional[Path] = None):
        """
        Initialize the DatabaseManager.

        Args:
            base_dir (Optional[Path]): The root directory for storing database files.
                                       Defaults to the application cache root.
        """
        self.base_dir = base_dir or PathUtils.get_cache_root()
        # Track open connections for cleanup purposes
        self._open_connections: Dict[str, List[sqlite3.Connection]] = defaultdict(list)
        self._conn_lock = threading.Lock()
        logger.debug("DatabaseManager initialized at: %s", self.base_dir)

    # --- CONNECTION METHODS ---

    def get_connection(self, project_id: int) -> sqlite3.Connection:
        """
        Retrieves a thread-local SQLite connection for the specified project.
        Creates the database directory if it does not exist.
        """
        db_path = PathUtils.get_project_db_path(project_id, self.base_dir)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_path_str = str(db_path)

        # Initialize thread-local storage if not present
        if not hasattr(thread_local_storage, 'connections'):
            thread_local_storage.connections = {}

        # Check for an existing healthy connection in this thread
        if db_path_str in thread_local_storage.connections:
            cached_conn = thread_local_storage.connections[db_path_str]
            try:
                cached_conn.execute("SELECT 1;")
                return cached_conn
            except sqlite3.Error:
                # Connection is dead, remove it and reconnect
                thread_local_storage.connections.pop(db_path_str, None)

        # Create a new connection
        try:
            conn = sqlite3.connect(
                db_path_str,
                isolation_level=None,  # Autocommit mode
                check_same_thread=False
            )
            # Optimize SQLite performance settings
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys = ON;")

            # Cache the connection for this thread
            thread_local_storage.connections[db_path_str] = conn

            # Track global connections safely
            with self._conn_lock:
                self._open_connections[db_path_str].append(conn)
            return conn
        except sqlite3.Error as e:
            logger.error(f"Fatal error opening DB {db_path_str}: {e}", exc_info=True)
            raise

    def close_project_connections(self, project_id: int) -> None:
        """Closes all open connections and forces a WAL checkpoint to clean up files."""
        db_path_str = str(PathUtils.get_project_db_path(project_id, self.base_dir))
        with self._conn_lock:
            if db_path_str in self._open_connections:
                connections = self._open_connections.pop(db_path_str)
                for conn in connections:
                    try:
                        # FORCEER CHECKPOINT: Verplaats data van WAL naar DB-bestand
                        # 'TRUNCATE' zet de WAL-file terug naar 0 bytes.
                        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                        conn.close()
                    except Exception as e:
                        logger.debug(f"Could not checkpoint/close connection: {e}")

        # Clear current thread's cache
        if hasattr(thread_local_storage, 'connections'):
            thread_local_storage.connections.pop(db_path_str, None)

        logger.info(f"✅ Connections for project {project_id} closed and WAL files truncated.")

    # --- EXECUTION METHODS ---

    def execute_query(self, project_id: int, query: str, params: tuple = ()) -> None:
        """Executes a single SQL query that does not return data (e.g., UPDATE, DELETE)."""
        conn = self.get_connection(project_id)
        try:
            with conn:
                conn.execute(query, params)
        except sqlite3.Error as e:
            logger.error(f"Query failed: {e} | Query: {query}")
            raise

    def execute_insert(self, project_id: int, query: str, params: tuple = ()) -> int:
        """
        Executes an INSERT statement and returns the `lastrowid`.
        Returns -1 on failure.
        """
        conn = self.get_connection(project_id)
        try:
            with conn:
                cursor = conn.execute(query, params)
                return cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Insert failed: {e}")
            return -1

    def execute_script(self, project_id: int, script: str) -> None:
        """Executes a raw SQL script (multiple statements)."""
        conn = self.get_connection(project_id)
        try:
            with conn:
                conn.executescript(script)
        except sqlite3.Error as e:
            logger.error(f"Script execution failed: {e}")
            raise

    # --- READ METHODS ---

    def fetch_all(self, project_id: int, query: str, params: tuple = ()) -> List[tuple]:
        """Executes a query and returns all rows as a list of tuples."""
        conn = self.get_connection(project_id)
        try:
            cursor = conn.execute(query, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Fetch failed: {e}")
            return []

    def fetch_one(self, project_id: int, query: str, params: tuple = ()) -> Optional[tuple]:
        """Executes a query and returns a single row, or None."""
        conn = self.get_connection(project_id)
        try:
            cursor = conn.execute(query, params)
            return cursor.fetchone()
        except sqlite3.Error:
            return None

    # --- WRITE METHODS---

    def save_batch(self, project_id: int, sql_query: str, data_tuples: List[tuple]) -> None:
        """
        Executes a synchronous batch insert using `executemany`.
        Ideal for high-throughput data ingestion.
        """
        if not data_tuples:
            return
        conn = self.get_connection(project_id)

        try:
            with conn:
                conn.executemany(sql_query, data_tuples)
        except sqlite3.Error as e:
            logger.error(f"Batch execution failed: {e}")
            if data_tuples:
                logger.debug(f"Sample tuple: {data_tuples[0]}")
            raise

    def clear_tables(self, project_id: int, table_names: List[str]) -> None:
        """Truncates (deletes all rows from) the specified tables."""
        if not table_names:
            return
        conn = self.get_connection(project_id)
        try:
            with conn:
                for table in table_names:
                    conn.execute(f"DELETE FROM {table}")
            logger.debug(
                f"Cleared tables for project {project_id}: {', '.join(table_names)}"
            )
        except sqlite3.Error as e:
            logger.error(f"Failed to clear tables {table_names}: {e}")

    # --- SCHEMA METHODS ---

    def get_schema_info(self, project_id: int) -> Dict[str, pd.DataFrame]:
        """
        Retrieves schema information for all tables in the database.
        Returns a dictionary mapping table names to their PRAGMA table_info DataFrames.
        """
        try:
            conn = self.get_connection(project_id)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            )
            tables = [r[0] for r in cursor.fetchall()]
            info = {}
            for t in tables:
                df = pd.read_sql_query(f"PRAGMA table_info({t})", conn)
                info[t] = df
            return info
        except Exception:
            return {}

    def init_schema(self, project_id: int) -> None:
        """
        Initializes the database schema using the standard DEFAULT_SCHEMA_SCRIPT.
        This ensures all projects use the exact same table structure.
        """
        self.execute_script(project_id, DEFAULT_SCHEMA_SCRIPT)