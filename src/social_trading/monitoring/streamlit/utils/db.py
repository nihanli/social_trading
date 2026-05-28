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

import datetime
import os
import subprocess

import pandas as pd
import psycopg2
import streamlit as st

# Local timezone (used to convert UTC datetimes for display)
_LOCAL_TZ = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo


def _get_local_iana_tz() -> str:
    """
    Return the local IANA timezone name (e.g. 'America/Los_Angeles').
    Falls back to a UTC-offset string if the system symlink can't be resolved.
    """
    try:
        link = subprocess.run(
            ["readlink", "/etc/localtime"], capture_output=True, text=True
        ).stdout.strip()
        # Resolve path components after 'zoneinfo'
        parts = link.split("/")
        for i, part in enumerate(parts):
            if part == "zoneinfo":
                return "/".join(parts[i + 1 :])
    except Exception:
        pass
    # Fallback: use UTC offset in ISO form but flip sign for POSIX (PostgreSQL quirk)
    # Use 'UTC' as safe default rather than a broken offset
    return "UTC"


_PG_TZ = _get_local_iana_tz()


@st.cache_resource
def get_connection():
    """
    Return a cached psycopg2 connection.
    Cached at process level — one connection per Streamlit worker.
    Session timezone is set to the local machine's IANA timezone (e.g.
    'America/Los_Angeles') so that TO_CHAR() calls in queries format
    timestamps in local time.
    """
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "trading"),
        user=os.getenv("DB_USER", "trader"),
        password=os.getenv("DB_PASSWORD", "changeme"),
    )
    with conn.cursor() as cur:
        cur.execute(f"SET TIME ZONE '{_PG_TZ}'")
    conn.commit()
    return conn


def localize_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all timezone-aware datetime columns in *df* to the local timezone.
    Timezone-naive columns are assumed UTC and are first localized to UTC.
    Returns the dataframe in-place (also returns it for chaining).
    """
    for col in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")
        df[col] = df[col].dt.tz_convert(_LOCAL_TZ)
    return df


def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """
    Execute *sql* and return results as a DataFrame.
    Returns an empty DataFrame on any database error.
    Rolls back and reconnects automatically if the connection is in a broken
    transaction state (e.g. after a failed query on a missing table).
    """
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception as exc:
        # Roll back the broken transaction so the connection is reusable
        try:
            conn.rollback()
        except Exception:
            # Connection is dead — clear the cache so next call reconnects
            get_connection.clear()
        st.warning(f"Database query failed: {exc}")
        return pd.DataFrame()
