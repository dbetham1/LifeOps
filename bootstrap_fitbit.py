import os
import base64
import secrets
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlparse, parse_qs

import requests
from dotenv import load_dotenv

AUTH_URL = "https://www.fitbit.com/oauth2/authorize"
TOKEN_URL = "https://api.fitbit.com/oauth2/token"

ROOT_DIR = Path(__file__).resolve().parent
ENV_PATH = ROOT_DIR / ".env"


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing {name} in {ENV_PATH}")
    return v


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
    os.environ[key] = value  # update current process env too


class CallbackHandler(BaseHTTPRequestHandler):
    auth_code: str | None = None
    error: str | None = None
    returned_state: str | None = None

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return

        qs = parse_qs(parsed.query)

        if "error" in qs:
            CallbackHandler.error = qs["error"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(f"Fitbit auth error: {CallbackHandler.error}".encode("utf-8"))
            return

        CallbackHandler.returned_state = qs.get("state", [None])[0]

        if "code" in qs:
            CallbackHandler.auth_code = qs["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Fitbit authorization received. You can close this tab.")
            return

        self.send_response(400)
        self.end_headers()
        self.wfile.write(b"No code found in callback.")

    def log_message(self, format, *args):
        return  # silence logs


def build_auth_url(client_id: str, redirect_uri: str, scopes: list[str], state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": " ".join(scopes),
        "state": state,
    }
    return AUTH_URL + "?" + urlencode(params)


def exchange_code_for_tokens(client_id: str, client_secret: str, redirect_uri: str, code: str) -> dict:
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Token exchange failed ({r.status_code}): {r.text}")
    return r.json()


def main():
    # Make .env authoritative for this process (critical)
    load_dotenv(dotenv_path=ENV_PATH, override=True)

    client_id = require_env("FITBIT_CLIENT_ID")
    client_secret = require_env("FITBIT_CLIENT_SECRET")
    redirect_uri = require_env("FITBIT_REDIRECT_URI")

    # Validate redirect uri and bind to it
    ru = urlparse(redirect_uri)
    if ru.scheme not in ("http", "https"):
        raise RuntimeError(f"FITBIT_REDIRECT_URI must be http(s): {redirect_uri}")
    if ru.path != "/callback":
        raise RuntimeError(f"FITBIT_REDIRECT_URI path must be /callback, got: {ru.path}")
    host = ru.hostname or "127.0.0.1"
    port = ru.port or (443 if ru.scheme == "https" else 80)

    scopes = ["profile", "activity", "heartrate", "sleep", "weight"]

    state = secrets.token_urlsafe(24)
    auth_url = build_auth_url(client_id, redirect_uri, scopes, state)

    print("\n1) Open this URL in your browser and approve access:\n")
    print(auth_url)
    print("\n2) Fitbit will redirect to:")
    print(f"   {redirect_uri}")
    print(f"\nWaiting locally on {host}:{port} ...\n")

    server = HTTPServer((host, port), CallbackHandler)

    try:
        while CallbackHandler.auth_code is None and CallbackHandler.error is None:
            server.handle_request()
    finally:
        server.server_close()

    if CallbackHandler.error:
        raise RuntimeError(f"Fitbit auth error: {CallbackHandler.error}")

    if CallbackHandler.returned_state != state:
        raise RuntimeError("OAuth state mismatch (possible stale callback or interference). Re-run bootstrap.")

    code = CallbackHandler.auth_code
    if not code:
        raise RuntimeError("No auth code captured.")

    tokens = exchange_code_for_tokens(client_id, client_secret, redirect_uri, code)

    refresh_token = tokens.get("refresh_token")
    user_id = tokens.get("user_id")

    if not refresh_token:
        raise RuntimeError(f"No refresh_token returned. Response: {tokens}")

    update_env_value("FITBIT_REFRESH_TOKEN", refresh_token, env_path=ENV_PATH)
    if user_id:
        update_env_value("FITBIT_USER_ID", str(user_id), env_path=ENV_PATH)

    print("\nSaved FITBIT_REFRESH_TOKEN to .env")
    if user_id:
        print(f"Saved FITBIT_USER_ID to .env: {user_id}")
    print("Done.\n")


if __name__ == "__main__":
    main()