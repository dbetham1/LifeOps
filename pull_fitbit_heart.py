"""
pull_fitbit_heart.py

LifeOps — Fitbit daily heart (resting HR) -> Parquet (upsert by date).

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
PARQUET_PATH = DATA_DIR / "raw_fitbit_heart_daily.parquet"


# ----------------------------
# Utilities
# ----------------------------
def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ----------------------------
# Fitbit API: heart (daily)
# ----------------------------
def fetch_heart_daily(access_token: str, user_id: str, start: date, end: date) -> list[tuple[str, int]]:
    """
    Returns list of (date_iso, resting_hr_int)

    Endpoint returns:
      activities-heart: [{dateTime: "YYYY-MM-DD", value: {restingHeartRate: N, ...}}, ...]
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{API_BASE}/1/user/{user_id}/activities/heart/date/{start.isoformat()}/{end.isoformat()}.json"

    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Fitbit heart fetch failed ({r.status_code}): {r.text}")

    payload = r.json()
    series = payload.get("activities-heart", [])

    out: list[tuple[str, int]] = []
    for item in series:
        ds = item.get("dateTime")
        resting = (item.get("value") or {}).get("restingHeartRate")
        if not ds or resting is None:
            continue
        try:
            out.append((ds, int(resting)))
        except (ValueError, TypeError):
            continue

    return out


# ----------------------------
# DuckDB merge -> Parquet
# ----------------------------
def connect_mem() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_progress_bar=false;")
    return con


def upsert_to_parquet(con: duckdb.DuckDBPyConnection, rows: list[tuple[str, int]]) -> tuple[int, int]:
    """
    Upsert rows into Parquet by date.
    Schema:
        date DATE (primary key semantics)
        resting_hr INTEGER
        ingested_at TIMESTAMPTZ
    """
    ensure_dirs()

    con.execute("CREATE TEMP TABLE incoming_heart (date DATE, resting_hr INTEGER);")
    con.executemany("INSERT INTO incoming_heart VALUES (?, ?);", rows)

    if PARQUET_PATH.exists():
        con.execute(f"""
            CREATE TEMP TABLE existing_heart AS
            SELECT
                date::DATE AS date,
                resting_hr::INTEGER AS resting_hr,
                ingested_at::TIMESTAMPTZ AS ingested_at
            FROM read_parquet('{PARQUET_PATH.as_posix()}');
        """)
    else:
        con.execute("""
            CREATE TEMP TABLE existing_heart (
                date DATE,
                resting_hr INTEGER,
                ingested_at TIMESTAMPTZ
            );
        """)

    ingested_at = utc_now()

    con.execute("""
        CREATE TEMP TABLE merged_heart AS
        SELECT e.date, e.resting_hr, e.ingested_at
        FROM existing_heart e
        LEFT JOIN incoming_heart i
          ON e.date = i.date
        WHERE i.date IS NULL

        UNION ALL

        SELECT i.date, i.resting_hr, ?::TIMESTAMPTZ AS ingested_at
        FROM incoming_heart i
    """, [ingested_at])

    con.execute(f"""
        COPY (
            SELECT date, resting_hr, ingested_at
            FROM merged_heart
            ORDER BY date
        )
        TO '{PARQUET_PATH.as_posix()}'
        (FORMAT PARQUET);
    """)

    final_total = con.execute("SELECT COUNT(*) FROM merged_heart;").fetchone()[0]

    con.execute("DROP TABLE incoming_heart;")
    con.execute("DROP TABLE existing_heart;")
    con.execute("DROP TABLE merged_heart;")

    return len(rows), int(final_total)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    end = date.today()
    start = end - timedelta(days=30)

    print(f"LifeOps: pulling Fitbit heart... {start} -> {end}")
    print(f"Token cache: {TOKEN_CACHE_PATH}")
    print(f"Parquet output: {PARQUET_PATH}")

    access_token, user_id = get_valid_access_token()

    rows = fetch_heart_daily(access_token, user_id, start, end)
    print(f"Fetched {len(rows)} days from Fitbit API.")

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