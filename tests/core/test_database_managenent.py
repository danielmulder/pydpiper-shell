from __future__ import annotations
import sqlite3
from datetime import datetime, timezone
from typing import List
import pandas as pd
import pytest
from pydpiper_shell.core.managers.database_manager import DatabaseManager

PROJECT_ID = 99


@pytest.fixture
def dm() -> DatabaseManager:
    m = DatabaseManager()
    m.init_schema(PROJECT_ID)
    return m


def _fetch_all(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> List[tuple]:
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def test_save_batch_persistence(dm: DatabaseManager):
    conn = dm.get_connection(PROJECT_ID)
    now_iso = datetime.now(timezone.utc).isoformat()

    dm.execute_query(PROJECT_ID, "DELETE FROM pages")

    # Added created_at to satisfy NOT NULL constraint
    sql_project = "INSERT OR REPLACE INTO project (id, name, start_url, run_mode, created_at) VALUES (?, ?, ?, ?, ?)"
    project_data = [(PROJECT_ID, "test_project", "https://example.com", "discovery", now_iso)]
    dm.save_batch(PROJECT_ID, sql_project, project_data)

    sql_pages = "INSERT OR REPLACE INTO pages (url, status_code, content, crawled_at) VALUES (?, ?, ?, ?)"
    pages_data = [("https://example.com/a", 200, "content", now_iso)]
    dm.save_batch(PROJECT_ID, sql_pages, pages_data)

    assert len(_fetch_all(conn, "SELECT url FROM pages")) == 1


def test_clear_data_preserves_project_shell(dm: DatabaseManager):
    conn = dm.get_connection(PROJECT_ID)
    now_iso = datetime.now(timezone.utc).isoformat()

    # Use clear_audit_data (or whatever is present in your Manager)
    clear_method = getattr(dm, 'clear_tables', getattr(dm, 'clear_data', None))
    if not clear_method:
        pytest.fail("DatabaseManager has no clear_data or clear_audit_data method")

    dm.save_batch(PROJECT_ID, "INSERT INTO pages (url, status_code, crawled_at) VALUES (?, ?, ?)",
                  [("https://example.com/wipe", 200, now_iso)])

    clear_method(PROJECT_ID, ["pages"])
    assert _fetch_all(conn, "SELECT COUNT(*) FROM pages")[0][0] == 0