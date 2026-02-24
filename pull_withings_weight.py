import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import base64

import requests
import duckdb
from dotenv import load_dotenv

# ----------------------------
# Paths / Env
# ----------------------------
ROOT_DIR = Path(__file__).resolve().parent
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

DATA_DIR = ROOT_DIR / "data"
PARQUET_PATH = DATA_DIR / "raw_withings_weight.parquet"

# ----------------------------
# Withings API
# ----------------------------
OAUTH_URL = "https://wbsapi.withings.net/v2/oauth2"
MEASURE_URL = "https://wbsapi.withings.net/measure"

MEAS_TYPE_WEIGHT = 1  # Weight


# ----------------------------
# Helpers
# ----------------------------
def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing {name} in {ENV_PATH}")
    return v


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def update_env_value(key: str, value: str, env_path: Path = ENV_PATH) -> None:
    """
    Updates/creates a key in .env. Keeps other lines intact.
    """
    lines: list[str] = []
    if env_path.exists():
        lines = env_path.read_text(encoding="utf-8").splitlines()

    found = False
    out: list[str] = []
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}")
            found = True
        else:
            out.append(line)

    if not found:
        out.append(f"{key}={value}")

    env_path.write_text("\n".join(out) + "\n", encoding="utf-8")
    os.environ[key] = value


def refresh_access_token() -> dict:
    """
    Uses the stored refresh token to get a new access token.
    Withings returns JSON: {"status":0,"body":{...}}
    """
    client_id = require_env("WITHINGS_CLIENT_ID")
    client_secret = require_env("WITHINGS_CLIENT_SECRET")
    refresh_token = require_env("WITHINGS_REFRESH_TOKEN")

    payload = {
        "action": "requesttoken",
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }

    r = requests.post(OAUTH_URL, data=payload, timeout=30)
    r.raise_for_status()

    data = r.json()
    if data.get("status") != 0:
        raise RuntimeError(f"Withings token refresh failed: {data}")

    body = data["body"]

    # Withings often rotates refresh tokens; persist it if it changes
    new_refresh = body.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
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
        # Optional paging/range:
        # "startdate": <unix_ts>,
        # "enddate": <unix_ts>,
    }

    r = requests.post(MEASURE_URL, headers=headers, data=payload, timeout=30)
    r.raise_for_status()

    data = r.json()
    if data.get("status") != 0:
        raise RuntimeError(f"Withings getmeas failed: {data}")

    return data["body"].get("measuregrps", [])


def extract_weight_rows(measure_groups: list[dict]) -> list[tuple]:
    """
    Returns rows:
      (measured_at_utc_datetime, weight_kg, grpid, attrib, category)
    """
    rows: list[tuple] = []

    for grp in measure_groups:
        measured_ts = grp.get("date")  # unix seconds (UTC)
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

            # Withings: value * 10^unit (for weight this is typically kg)
            weight_kg = float(value) * (10 ** int(unit))

            measured_at = datetime.fromtimestamp(int(measured_ts), tz=timezone.utc)

            rows.append((measured_at, weight_kg, grpid, attrib, category))

    return rows


def connect_mem() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA enable_progress_bar=false;")
    return con


def upsert_to_parquet(con: duckdb.DuckDBPyConnection, rows: list[tuple]) -> tuple[int, int]:
    """
    Upsert rows into Parquet by measured_at (PRIMARY KEY semantics).

    Parquet schema:
      measured_at TIMESTAMPTZ
      weight_kg DOUBLE
      grpid BIGINT
      attrib INTEGER
      category INTEGER
      ingested_at TIMESTAMPTZ
    """
    ensure_dirs()

    # Stage incoming
    con.execute("""
        CREATE TEMP TABLE incoming_weight (
            measured_at TIMESTAMPTZ,
            weight_kg DOUBLE,
            grpid BIGINT,
            attrib INTEGER,
            category INTEGER
        );
    """)
    con.executemany("INSERT INTO incoming_weight VALUES (?, ?, ?, ?, ?);", rows)

    # Load existing parquet (or empty)
    if PARQUET_PATH.exists():
        con.execute(f"""
            CREATE TEMP TABLE existing_weight AS
            SELECT
                measured_at::TIMESTAMPTZ AS measured_at,
                weight_kg::DOUBLE AS weight_kg,
                grpid::BIGINT AS grpid,
                attrib::INTEGER AS attrib,
                category::INTEGER AS category,
                ingested_at::TIMESTAMPTZ AS ingested_at
            FROM read_parquet('{PARQUET_PATH.as_posix()}');
        """)
    else:
        con.execute("""
            CREATE TEMP TABLE existing_weight (
                measured_at TIMESTAMPTZ,
                weight_kg DOUBLE,
                grpid BIGINT,
                attrib INTEGER,
                category INTEGER,
                ingested_at TIMESTAMPTZ
            );
        """)

    ingested_at = datetime.now(timezone.utc)

    # Upsert:
    # - keep existing rows not present in incoming
    # - add incoming rows with fresh ingested_at
    con.execute("""
        CREATE TEMP TABLE merged_weight AS
        SELECT e.measured_at, e.weight_kg, e.grpid, e.attrib, e.category, e.ingested_at
        FROM existing_weight e
        LEFT JOIN incoming_weight i
          ON e.measured_at = i.measured_at
        WHERE i.measured_at IS NULL

        UNION ALL

        SELECT i.measured_at, i.weight_kg, i.grpid, i.attrib, i.category, ?::TIMESTAMPTZ AS ingested_at
        FROM incoming_weight i
    """, [ingested_at])

    # Persist (overwrite)
    con.execute(f"""
        COPY (
            SELECT
                measured_at,
                weight_kg,
                grpid,
                attrib,
                category,
                ingested_at
            FROM merged_weight
            ORDER BY measured_at
        )
        TO '{PARQUET_PATH.as_posix()}'
        (FORMAT PARQUET);
    """)

    final_total = con.execute("SELECT COUNT(*) FROM merged_weight;").fetchone()[0]

    # Cleanup
    con.execute("DROP TABLE incoming_weight;")
    con.execute("DROP TABLE existing_weight;")
    con.execute("DROP TABLE merged_weight;")

    return len(rows), int(final_total)


def main() -> None:
    print("LifeOps: pulling Withings weight...")
    print(f"Using .env: {ENV_PATH}")
    print(f"Parquet output: {PARQUET_PATH}")

    token_body = refresh_access_token()
    access_token = token_body.get("access_token")
    if not access_token:
        raise RuntimeError(f"No access_token in refresh response: {token_body}")

    measure_groups = fetch_weight_measure_groups(access_token)
    rows = extract_weight_rows(measure_groups)

    con = connect_mem()
    try:
        incoming, final_total = upsert_to_parquet(con, rows)
    finally:
        con.close()

    print(f"Fetched {len(measure_groups)} measure groups.")
    print(f"Extracted {len(rows)} weight rows.")
    print(f"Upserted {incoming} rows. Parquet total rows now: {final_total}.")
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nERROR:", str(e))
        sys.exit(1)