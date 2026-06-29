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

Timezone convention
-------------------
* All timestamps are stored as TIMESTAMP WITH TIME ZONE (UTC) in PostgreSQL.
* The DB session timezone is set to the local IANA timezone (LOCAL_TZ_NAME)
  so TO_CHAR() display and CURRENT_DATE reflect local wall-clock time.
* Python datetime handling always parses into UTC first (utc=True), then
  converts to LOCAL_TZ (a zoneinfo-backed, DST-aware zone) for display.
* Use UTC midnight anchors for "today" filters:
      col >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
  This is consistent regardless of the local clock and matches the signals
  date filter already in use throughout the app.
"""
from __future__ import annotations

import os
import subprocess
import zoneinfo

import pandas as pd
import psycopg2
import streamlit as st


def _get_local_iana_tz() -> str:
    """Return the local IANA timezone name (e.g. 'America/Los_Angeles').
    Falls back to 'UTC' if the system symlink cannot be resolved."""
    try:
        link = subprocess.run(
            ["readlink", "/etc/localtime"], capture_output=True, text=True
        ).stdout.strip()
        parts = link.split("/")
        for i, part in enumerate(parts):
            if part == "zoneinfo":
                return "/".join(parts[i + 1:])
    except Exception:
        pass
    return "UTC"


# Shared IANA timezone name used by both the DB session and Python conversions.
# Import this in other modules (e.g. chart_data.py) instead of hardcoding a tz.
LOCAL_TZ_NAME: str = _get_local_iana_tz()

# DST-aware zoneinfo object — use this for all pandas .dt.tz_convert() calls
# so that historical timestamps show the correct UTC offset (e.g. PST vs PDT).
LOCAL_TZ = zoneinfo.ZoneInfo(LOCAL_TZ_NAME)

# Keep as alias so PostgreSQL SET TIME ZONE uses the same IANA name.
_PG_TZ = LOCAL_TZ_NAME


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
    Convert all datetime columns in *df* to LOCAL_TZ (DST-aware local timezone).
    Timezone-naive columns are assumed UTC and are first localized to UTC.
    Timezone-aware columns (any offset) are converted via UTC to LOCAL_TZ.
    Returns the dataframe in-place (also returns it for chaining).
    """
    for col in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        if df[col].dt.tz is None:
            df[col] = df[col].dt.tz_localize("UTC")
        df[col] = df[col].dt.tz_convert(LOCAL_TZ)
    return df


def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """
    Execute *sql* and return results as a DataFrame.
    Returns an empty DataFrame on any database error.
    Rolls back and reconnects automatically if the connection is in a broken
    transaction state (e.g. after a failed query on a missing table).

    Uses a raw psycopg2 cursor rather than pd.read_sql() to avoid the
    "DBAPI2 objects are not tested" UserWarning that pandas emits when passed
    a bare psycopg2 connection.

    Commits after every query so that CURRENT_DATE / NOW() are always
    evaluated against the current wall-clock time rather than the start of a
    long-lived transaction (psycopg2 default autocommit=False keeps the same
    transaction open indefinitely, freezing CURRENT_DATE at the first query
    of the session).
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        conn.commit()
        return pd.DataFrame(rows, columns=cols)
    except Exception as exc:
        # Roll back the broken transaction so the connection is reusable
        try:
            conn.rollback()
        except Exception:
            # Connection is dead — clear the cache so next call reconnects
            get_connection.clear()
        st.warning(f"Database query failed: {exc}")
        return pd.DataFrame()
