import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
import duckdb
from dotenv import load_dotenv

# ----------------------------
# Config
# ----------------------------
load_dotenv()

CLIENT_ID = os.getenv("WITHINGS_CLIENT_ID")
CLIENT_SECRET = os.getenv("WITHINGS_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("WITHINGS_REFRESH_TOKEN")

OAUTH_URL = "https://wbsapi.withings.net/v2/oauth2"
MEASURE_URL = "https://wbsapi.withings.net/measure"

# Persist DB in a stable, repo-relative location
DB_PATH = Path("data") / "lifeops.duckdb"

# Withings measure type constants
MEAS_TYPE_WEIGHT = 1  # Weight


# ----------------------------
# Helpers
# ----------------------------
def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing {name} in .env")
    return v


def update_env_value(key: str, value: str, env_path: str = ".env") -> None:
    """
    Updates/creates a key in .env. Keeps other lines intact.
    """
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()

    found = False
    out = []
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}")
            found = True
        else:
            out.append(line)

    if not found:
        out.append(f"{key}={value}")

    with open(env_path, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")

    # Update process env immediately
    os.environ[key] = value

    # Keep module-global REFRESH_TOKEN in sync
    global REFRESH_TOKEN
    if key == "WITHINGS_REFRESH_TOKEN":
        REFRESH_TOKEN = value


def refresh_access_token() -> dict:
    """
    Uses the stored refresh token to get a new access token.
    Withings returns JSON: {"status":0,"body":{...}}
    """
    require_env("WITHINGS_CLIENT_ID")
    require_env("WITHINGS_CLIENT_SECRET")
    require_env("WITHINGS_REFRESH_TOKEN")

    payload = {
        "action": "requesttoken",
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    }

    r = requests.post(OAUTH_URL, data=payload, timeout=30)
    r.raise_for_status()

    data = r.json()
    if data.get("status") != 0:
        raise RuntimeError(f"Token refresh failed: {data}")

    body = data["body"]

    # Withings often rotates refresh tokens.
    new_refresh = body.get("refresh_token")
    if new_refresh and new_refresh != REFRESH_TOKEN:
        update_env_value("WITHINGS_REFRESH_TOKEN", new_refresh)

    return body


def fetch_weight_measure_groups(access_token: str) -> list[dict]:
    """
    Calls Withings Measure API to fetch measurement groups.
    Returns measuregrps list.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {
        "action": "getmeas",
        "meastype": MEAS_TYPE_WEIGHT,
        # Later you can add:
        # "startdate": <unix_ts>,
        # "enddate": <unix_ts>,
    }

    r = requests.post(MEASURE_URL, headers=headers, data=payload, timeout=30)
    r.raise_for_status()

    data = r.json()
    if data.get("status") != 0:
        raise RuntimeError(f"getmeas failed: {data}")

    return data["body"].get("measuregrps", [])


def connect_db() -> duckdb.DuckDBPyConnection:
    """
    Connects to a persistent, file-backed DuckDB database.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    # Optional: help with reproducibility / safety
    con.execute("PRAGMA enable_progress_bar=false;")
    return con


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Creates the persistent weight table.
    Store timestamps as TIMESTAMPTZ (UTC) so timezone conversion later is correct.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_withings_weight (
            measured_at TIMESTAMPTZ NOT NULL,
            weight_kg DOUBLE NOT NULL,
            grpid BIGINT,
            attrib INTEGER,
            category INTEGER,
            ingested_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY(measured_at)
        )
    """)


def upsert_weights(con: duckdb.DuckDBPyConnection, measure_groups: list[dict]) -> int:
    """
    Extracts weights and upserts into DuckDB by measured_at.
    Uses a TEMP staging table + MERGE for idempotency.
    """
    rows: list[tuple] = []

    # Ingest timestamp: UTC, as unix seconds (robust, unambiguous)
    ingested_ts = int(datetime.now(timezone.utc).timestamp())

    for grp in measure_groups:
        measured_ts = grp.get("date")  # unix seconds
        if not measured_ts:
            continue

        grpid = grp.get("grpid")
        attrib = grp.get("attrib")
        category = grp.get("category")

        for m in grp.get("measures", []):
            if m.get("type") != MEAS_TYPE_WEIGHT:
                continue

            value = m.get("value")
            unit = m.get("unit", 0)
            if value is None:
                continue

            # Withings: value * 10^unit
            weight_kg = float(value) * (10 ** int(unit))

            rows.append((
                int(measured_ts),
                weight_kg,
                grpid,
                attrib,
                category,
                int(ingested_ts),
            ))

    if not rows:
        return 0

    con.execute("""
        CREATE TEMP TABLE tmp_weight (
            measured_ts BIGINT,
            weight_kg DOUBLE,
            grpid BIGINT,
            attrib INTEGER,
            category INTEGER,
            ingested_ts BIGINT
        )
    """)
    con.executemany("INSERT INTO tmp_weight VALUES (?, ?, ?, ?, ?, ?)", rows)

    con.execute("""
        MERGE INTO raw_withings_weight t
        USING (
            SELECT
                to_timestamp(measured_ts) AS measured_at,
                weight_kg,
                grpid,
                attrib,
                category,
                to_timestamp(ingested_ts) AS ingested_at
            FROM tmp_weight
        ) s
        ON t.measured_at = s.measured_at
        WHEN MATCHED THEN UPDATE SET
            weight_kg = s.weight_kg,
            grpid = s.grpid,
            attrib = s.attrib,
            category = s.category,
            ingested_at = s.ingested_at
        WHEN NOT MATCHED THEN INSERT
            (measured_at, weight_kg, grpid, attrib, category, ingested_at)
        VALUES
            (s.measured_at, s.weight_kg, s.grpid, s.attrib, s.category, s.ingested_at)
    """)

    con.execute("DROP TABLE tmp_weight")
    return len(rows)


def main() -> None:
    print("LifeOps: pulling Withings weight...")

    token_body = refresh_access_token()
    access_token = token_body.get("access_token")
    if not access_token:
        raise RuntimeError(f"No access_token in refresh response: {token_body}")

    measure_groups = fetch_weight_measure_groups(access_token)

    con = connect_db()
    try:
        ensure_schema(con)
        n = upsert_weights(con, measure_groups)
        total = con.execute("SELECT COUNT(*) FROM raw_withings_weight").fetchone()[0]
        db_list = con.execute("PRAGMA database_list;").fetchall()
    finally:
        con.close()

    print(f"Connected DB: {db_list}")
    print(f"Fetched {len(measure_groups)} measure groups.")
    print(f"Processed {n} weight rows (pre-dedupe count).")
    print(f"DB file: {DB_PATH} | raw_withings_weight rows: {total}")
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nERROR:", str(e))
        sys.exit(1)