"""
pull_fitbit_sleep.py

LifeOps — Fitbit daily sleep -> Parquet (upsert by date).

Auth:
- Uses shared fitbit_auth.py for OAuth refresh-token rotation + token cache + lock.
- Bootstrap only needed when refresh token becomes invalid/revoked.

Requirements:
    pip install requests duckdb python-dotenv
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import requests

from fitbit_auth import get_valid_access_token, TOKEN_CACHE_PATH  # TOKEN_CACHE_PATH optional but handy for logging


# ----------------------------
# Config
# ----------------------------
API_BASE = "https://api.fitbit.com"

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
PARQUET_PATH = DATA_DIR / "raw_fitbit_sleep_daily.parquet"


# ----------------------------
# Utilities
# ----------------------------
def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ----------------------------
# Fitbit API: sleep (daily)
# ----------------------------
def fetch_sleep_daily(access_token: str, user_id: str, day: date) -> tuple | None:
    """
    Returns one row:
      (date_iso, minutes_asleep, minutes_in_bed, efficiency, minutes_deep, minutes_light, minutes_rem, minutes_wake)

    If no sleep log exists for the day, returns None.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{API_BASE}/1.2/user/{user_id}/sleep/date/{day.isoformat()}.json"

    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Fitbit sleep fetch failed ({r.status_code}): {r.text}")

    payload = r.json()

    summary = payload.get("summary") or {}

    minutes_asleep = summary.get("totalMinutesAsleep")
    minutes_in_bed = summary.get("totalTimeInBed")
    efficiency = summary.get("efficiency")

    stages = summary.get("stages") or {}
    minutes_deep = stages.get("deep")
    minutes_light = stages.get("light")
    minutes_rem = stages.get("rem")
    minutes_wake = stages.get("wake")

    if minutes_asleep is None and minutes_in_bed is None and efficiency is None:
        return None

    def to_int(x):
        try:
            return int(x) if x is not None else None
        except (ValueError, TypeError):
            return None

    return (
        day.isoformat(),
        to_int(minutes_asleep),
        to_int(minutes_in_bed),
        to_int(efficiency),
        to_int(minutes_deep),
        to_int(minutes_light),
        to_int(minutes_rem),
        to_int(minutes_wake),
    )


def fetch_sleep_range(access_token: str, user_id: str, start: date, end: date) -> list[tuple]:
    """
    Fetch sleep for each day in [start, end] inclusive.
    """
    rows: list[tuple] = []
    d = start
    while d <= end:
        row = fetch_sleep_daily(access_token, user_id, d)
        if row is not None:
            rows.append(row)
        d += timedelta(days=1)
    return rows


# ----------------------------
# DuckDB merge -> Parquet
# ----------------------------
def connect_mem() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_progress_bar=false;")
    return con


def upsert_to_parquet(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> tuple[int, int]:
    """
    Upsert rows into Parquet by date.

    Schema:
        date DATE (primary key semantics)
        minutes_asleep INTEGER
        minutes_in_bed INTEGER
        efficiency INTEGER
        minutes_deep INTEGER
        minutes_light INTEGER
        minutes_rem INTEGER
        minutes_wake INTEGER
        ingested_at TIMESTAMPTZ
    """
    ensure_dirs()

    con.execute("""
        CREATE TEMP TABLE incoming_sleep (
            date DATE,
            minutes_asleep INTEGER,
            minutes_in_bed INTEGER,
            efficiency INTEGER,
            minutes_deep INTEGER,
            minutes_light INTEGER,
            minutes_rem INTEGER,
            minutes_wake INTEGER
        );
    """)
    con.executemany("INSERT INTO incoming_sleep VALUES (?, ?, ?, ?, ?, ?, ?, ?);", rows)

    if PARQUET_PATH.exists():
        con.execute(f"""
            CREATE TEMP TABLE existing_sleep AS
            SELECT
                date::DATE AS date,
                minutes_asleep::INTEGER AS minutes_asleep,
                minutes_in_bed::INTEGER AS minutes_in_bed,
                efficiency::INTEGER AS efficiency,
                minutes_deep::INTEGER AS minutes_deep,
                minutes_light::INTEGER AS minutes_light,
                minutes_rem::INTEGER AS minutes_rem,
                minutes_wake::INTEGER AS minutes_wake,
                ingested_at::TIMESTAMPTZ AS ingested_at
            FROM read_parquet('{PARQUET_PATH.as_posix()}');
        """)
    else:
        con.execute("""
            CREATE TEMP TABLE existing_sleep (
                date DATE,
                minutes_asleep INTEGER,
                minutes_in_bed INTEGER,
                efficiency INTEGER,
                minutes_deep INTEGER,
                minutes_light INTEGER,
                minutes_rem INTEGER,
                minutes_wake INTEGER,
                ingested_at TIMESTAMPTZ
            );
        """)

    ingested_at = utc_now()

    con.execute("""
        CREATE TEMP TABLE merged_sleep AS
        SELECT e.*
        FROM existing_sleep e
        LEFT JOIN incoming_sleep i
          ON e.date = i.date
        WHERE i.date IS NULL

        UNION ALL

        SELECT
            i.date,
            i.minutes_asleep,
            i.minutes_in_bed,
            i.efficiency,
            i.minutes_deep,
            i.minutes_light,
            i.minutes_rem,
            i.minutes_wake,
            ?::TIMESTAMPTZ AS ingested_at
        FROM incoming_sleep i
    """, [ingested_at])

    con.execute(f"""
        COPY (
            SELECT
                date,
                minutes_asleep,
                minutes_in_bed,
                efficiency,
                minutes_deep,
                minutes_light,
                minutes_rem,
                minutes_wake,
                ingested_at
            FROM merged_sleep
            ORDER BY date
        )
        TO '{PARQUET_PATH.as_posix()}'
        (FORMAT PARQUET);
    """)

    final_total = con.execute("SELECT COUNT(*) FROM merged_sleep;").fetchone()[0]

    con.execute("DROP TABLE incoming_sleep;")
    con.execute("DROP TABLE existing_sleep;")
    con.execute("DROP TABLE merged_sleep;")

    return len(rows), int(final_total)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    end = date.today()
    start = end - timedelta(days=30)

    print(f"LifeOps: pulling Fitbit sleep... {start} -> {end}")
    print(f"Token cache: {TOKEN_CACHE_PATH}")
    print(f"Parquet output: {PARQUET_PATH}")

    access_token, user_id = get_valid_access_token()

    rows = fetch_sleep_range(access_token, user_id, start, end)
    print(f"Fetched {len(rows)} days with sleep summaries.")

    con = connect_mem()
    try:
        incoming, final_total = upsert_to_parquet(con, rows)
    finally:
        con.close()

    print(f"Upserted {incoming} rows. Parquet total rows now: {final_total}.")
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nERROR:", str(e))
        sys.exit(1)