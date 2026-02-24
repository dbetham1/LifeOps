from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Tuple

import requests
from dotenv import load_dotenv

TOKEN_URL = "https://api.fitbit.com/oauth2/token"

ROOT_DIR = Path(__file__).resolve().parent
ENV_PATH = ROOT_DIR / ".env"
DATA_DIR = ROOT_DIR / "data"
TOKEN_CACHE_PATH = DATA_DIR / "fitbit_token.json"
LOCK_PATH = DATA_DIR / ".fitbit_token.lock"


@dataclass(frozen=True)
class FitbitCreds:
    client_id: str
    client_secret: str


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing {name} in {ENV_PATH}")
    return v


class FileLock:
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


def load_token_cache() -> dict[str, Any] | None:
    if not TOKEN_CACHE_PATH.exists():
        return None
    return json.loads(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))


def save_token_cache(token: dict[str, Any]) -> None:
    ensure_dirs()
    tmp = TOKEN_CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(token, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(TOKEN_CACHE_PATH)


def seed_cache_from_env_once() -> None:
    """
    Seed token cache ONLY if missing. After that, .env refresh token is ignored.
    """
    if TOKEN_CACHE_PATH.exists():
        return

    refresh = require_env("FITBIT_REFRESH_TOKEN")
    save_token_cache({
        "refresh_token": refresh,
        "access_token": None,
        "user_id": os.getenv("FITBIT_USER_ID"),
        "expires_at_utc": None,
        "seeded_at_utc": dt_to_iso(utc_now()),
        "note": "Seeded from .env once. From now on, token cache is source of truth.",
    })


def refresh_fitbit_token(creds: FitbitCreds, refresh_token: str) -> dict[str, Any]:
    basic = base64.b64encode(f"{creds.client_id}:{creds.client_secret}".encode("utf-8")).decode("utf-8")
    headers = {"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"}
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
        "expires_in": expires_in,
        "expires_at_utc": expires_at_utc,
        "refreshed_at_utc": dt_to_iso(utc_now()),
    }


def get_valid_access_token() -> Tuple[str, str]:
    # Always load the same .env path, not whatever cwd happens to be
    load_dotenv(dotenv_path=ENV_PATH, override=True)

    creds = FitbitCreds(
        client_id=require_env("FITBIT_CLIENT_ID"),
        client_secret=require_env("FITBIT_CLIENT_SECRET"),
    )

    seed_cache_from_env_once()

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
            rt = cache.get("refresh_token")
            if not rt:
                raise RuntimeError(f"Token cache missing refresh_token: {TOKEN_CACHE_PATH}")

            new_state = refresh_fitbit_token(creds, rt)

            if not new_state.get("access_token") or not new_state.get("refresh_token"):
                raise RuntimeError(f"Refresh response missing tokens: {new_state}")

            if not new_state.get("user_id"):
                new_state["user_id"] = user_id

            save_token_cache({**cache, **new_state})
            access = new_state["access_token"]
            user_id = new_state.get("user_id") or user_id

        if not access or not user_id:
            raise RuntimeError(f"Token state missing access_token/user_id. Cache: {cache}")

        return str(access), str(user_id)