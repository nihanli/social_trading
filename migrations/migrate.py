#!/usr/bin/env python3
"""
Idempotent database migration runner.
Run: python migrations/migrate.py

Reads DB credentials from environment variables (or .env file).
Applies all .sql files in migrations/ in filename order.
Skips already-applied migrations using a tracking table.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

MIGRATIONS_DIR = Path(__file__).parent

TRACKING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename    VARCHAR(255) PRIMARY KEY,
    applied_at  TIMESTAMPTZ DEFAULT NOW()
);
"""


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "trading"),
        user=os.getenv("DB_USER", "trader"),
        password=os.getenv("DB_PASSWORD", ""),
        connect_timeout=10,
    )


def run_migrations() -> None:
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(TRACKING_TABLE_SQL)

        sql_files = sorted(
            f for f in MIGRATIONS_DIR.glob("*.sql")
            if f.name[0].isdigit()
        )

        for sql_file in sql_files:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM schema_migrations WHERE filename = %s",
                        (sql_file.name,),
                    )
                    if cur.fetchone():
                        print(f"  skip  {sql_file.name}")
                        continue

                    print(f"  apply {sql_file.name} ...", end=" ")
                    sql = sql_file.read_text()
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO schema_migrations (filename) VALUES (%s)",
                        (sql_file.name,),
                    )
                    print("OK")

        print("Migrations complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        run_migrations()
    except psycopg2.OperationalError as exc:
        msg = str(exc).strip()
        if "lock timeout" in msg:
            print(f"ERROR: Table lock timeout — the application is holding a lock on the signals table.", file=sys.stderr)
            print("Stop the signal service first, then re-run: python migrations/migrate.py", file=sys.stderr)
        else:
            print(f"ERROR: Cannot connect to database — {exc}", file=sys.stderr)
            print("Is postgres running? Try: make up", file=sys.stderr)
        sys.exit(1)
