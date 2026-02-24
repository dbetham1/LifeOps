"""
pull_fitbit_heart.py

LifeOps — Fitbit daily heart (resting HR) -> Parquet (upsert by date), with proper OAuth token rotation.

Key fixes:
- Fitbit refresh tokens rotate (single-use). Persist token state in local JSON cache.
- Optional file lock to prevent concurrent refresh/token corruption.
- DuckDB in-memory merge; Parquet overwritten.

Requirements:
    pip install requests duckdb python-dotenv

.env expected:
    FITBIT_CLIENT_ID=...
    FITBIT_CLIENT_SECRET=...
    FITBIT_REFRESH_TOKEN=...   # initial seed token (bootstrap once)
Optional:
    FITBIT_USER_ID=...         # not required; we use token response user_id
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests
from dotenv import load_dotenv

# ----------------------------
# Config
# ----------------------------
TOKEN_URL = "https://api.fitbit.com/oauth2/token"
API_BASE = "https://api.fitbit.com"

ROOT_DIR = Path(__file__).resolve().parent
ENV_PATH = ROOT_DIR / ".env"
DATA_DIR = ROOT_DIR / "data"

PARQUET_PATH = DATA_DIR / "raw_fitbit_heart_daily.parquet"
TOKEN_CACHE_PATH = DATA_DIR / "fitbit_token.json"
LOCK_PATH = DATA_DIR / ".fitbit_token.lock"  # local lock to avoid concurrency


@dataclass(frozen=True)
class FitbitCreds:
    client_id: str
    client_secret: str


# ----------------------------
# Utilities
# ----------------------------
def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing {name} in {ENV_PATH}")
    return v


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# ----------------------------
# Simple cross-platform lock (best-effort)
# ----------------------------
class FileLock:
    """
    Best-effort local lock. Prevents two runs from rotating/overwriting token cache simultaneously.
    """
    def __init__(self, path: Path, timeout_s: int = 60, poll_s: float = 0.2):
        self.path = path
        self.timeout_s = timeout_s
        self.poll_s = poll_s
        self._fh = None

    def acquire(self) -> None:
        ensure_dirs()
        start = time.time()
        while True:
            try:
                self._fh = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(self._fh, str(os.getpid()).encode("utf-8"))
                return
            except FileExistsError:
                if time.time() - start > self.timeout_s:
                    raise RuntimeError(f"Timed out waiting for lock: {self.path}")
                time.sleep(self.poll_s)

    def release(self) -> None:
        try:
            if self._fh is not None:
                os.close(self._fh)
                self._fh = None
            if self.path.exists():
                self.path.unlink()
        except Exception:
            pass

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


# ----------------------------
# Token cache
# ----------------------------
def load_token_cache() -> dict[str, Any] | None:
    if not TOKEN_CACHE_PATH.exists():
        return None
    try:
        return json.loads(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_token_cache(token: dict[str, Any]) -> None:
    ensure_dirs()
    tmp = TOKEN_CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(token, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(TOKEN_CACHE_PATH)


def seed_refresh_token_from_env_if_needed() -> None:
    """
    Ensure token cache exists and is consistent with .env refresh token.
    (If you re-bootstrap and .env refresh token changes, we reseed the cache and force a refresh.)
    """
    refresh_env = require_env("FITBIT_REFRESH_TOKEN")

    if not TOKEN_CACHE_PATH.exists():
        save_token_cache({
            "refresh_token": refresh_env,
            "access_token": None,
            "user_id": os.getenv("FITBIT_USER_ID"),
            "expires_at_utc": None,
            "seeded_at_utc": dt_to_iso(utc_now()),
            "note": "Seeded from .env. Will rotate on first refresh.",
        })
        return

    cache = load_token_cache() or {}

    if cache.get("refresh_token") != refresh_env:
        cache["refresh_token"] = refresh_env
        cache["access_token"] = None
        cache["expires_at_utc"] = None
        cache["reseeded_at_utc"] = dt_to_iso(utc_now())
        cache["note"] = "Refresh token updated from .env (re-bootstrap detected)."
        save_token_cache(cache)


# ----------------------------
# OAuth refresh
# ----------------------------
def refresh_fitbit_token(creds: FitbitCreds, refresh_token: str) -> dict[str, Any]:
    basic = base64.b64encode(f"{creds.client_id}:{creds.client_secret}".encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Fitbit token refresh failed ({r.status_code}): {r.text}")

    token = r.json()

    expires_in = token.get("expires_in")
    expires_at_utc = None
    if isinstance(expires_in, int):
        expires_at_utc = dt_to_iso(utc_now() + timedelta(seconds=expires_in))

    return {
        "access_token": token.get("access_token"),
        "refresh_token": token.get("refresh_token"),
        "user_id": token.get("user_id"),
        "scope": token.get("scope"),
        "token_type": token.get("token_type"),
        "expires_in": expires_in,
        "expires_at_utc": expires_at_utc,
        "refreshed_at_utc": dt_to_iso(utc_now()),
    }


def get_valid_access_token(creds: FitbitCreds) -> tuple[str, str]:
    """
    Returns (access_token, user_id). Refreshes if needed.
    """
    seed_refresh_token_from_env_if_needed()

    with FileLock(LOCK_PATH, timeout_s=60):
        cache = load_token_cache()
        if not cache:
            raise RuntimeError(f"Token cache missing/unreadable: {TOKEN_CACHE_PATH}")

        access = cache.get("access_token")
        user_id = cache.get("user_id")
        expires_at = cache.get("expires_at_utc")

        needs_refresh = True
        if access and expires_at:
            try:
                exp = iso_to_dt(expires_at)
                needs_refresh = utc_now() >= (exp - timedelta(minutes=2))
            except Exception:
                needs_refresh = True

        if needs_refresh:
            refresh_token = cache.get("refresh_token")
            if not refresh_token:
                raise RuntimeError(
                    "Token cache missing refresh_token. Re-bootstrap OAuth and set FITBIT_REFRESH_TOKEN in .env."
                )

            new_cache = refresh_fitbit_token(creds, refresh_token)

            if not new_cache.get("refresh_token"):
                raise RuntimeError(f"Refresh succeeded but response missing refresh_token: {new_cache}")
            if not new_cache.get("access_token"):
                raise RuntimeError(f"Refresh succeeded but response missing access_token: {new_cache}")
            if not new_cache.get("user_id"):
                new_cache["user_id"] = user_id

            save_token_cache({**cache, **new_cache})
            access = new_cache["access_token"]
            user_id = new_cache.get("user_id") or user_id

        if not access or not user_id:
            raise RuntimeError(f"Token state missing access_token/user_id. Cache: {cache}")

        return str(access), str(user_id)


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
    load_dotenv(dotenv_path=ENV_PATH, override=True)

    creds = FitbitCreds(
        client_id=require_env("FITBIT_CLIENT_ID"),
        client_secret=require_env("FITBIT_CLIENT_SECRET"),
    )

    end = date.today()
    start = end - timedelta(days=30)

    print(f"LifeOps: pulling Fitbit heart... {start} -> {end}")
    print(f"Using .env: {ENV_PATH}")
    print(f"Token cache: {TOKEN_CACHE_PATH}")
    print(f"Parquet output: {PARQUET_PATH}")

    access_token, user_id = get_valid_access_token(creds)

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