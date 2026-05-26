"""
PostgreSQL helpers for the Streamlit monitoring UI.

Provides a cached connection and a thin query() wrapper that returns
a pandas DataFrame.  Streamlit caches the connection object across
reruns so each page load doesn't open a new TCP connection.

Environment variables (from .env / docker-compose):
    DB_HOST       postgres  (default)
    DB_PORT       5432      (default)
    DB_NAME       trading   (default)
    DB_USER       trader    (default)
    DB_PASSWORD   changeme  (default)
"""
from __future__ import annotations

import os

import pandas as pd
import psycopg2
import streamlit as st


@st.cache_resource
def get_connection():
    """
    Return a cached psycopg2 connection.
    Cached at process level — one connection per Streamlit worker.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "trading"),
        user=os.getenv("DB_USER", "trader"),
        password=os.getenv("DB_PASSWORD", "changeme"),
    )


def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """
    Execute *sql* and return results as a DataFrame.
    Returns an empty DataFrame on any database error.
    """
    try:
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)
    except Exception as exc:
        st.warning(f"Database query failed: {exc}")
        return pd.DataFrame()
