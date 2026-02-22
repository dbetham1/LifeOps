import os
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import requests
from dotenv import load_dotenv

# === CONFIG ===
REDIRECT_URI = "http://localhost:8000/callback"
AUTHORIZE_URL = "https://account.withings.com/oauth2_user/authorize2"
TOKEN_URL = "https://wbsapi.withings.net/v2/oauth2"
SCOPE = "user.metrics,user.activity"
STATE = "lifeops"

load_dotenv()

CLIENT_ID = os.getenv("WITHINGS_CLIENT_ID")
CLIENT_SECRET = os.getenv("WITHINGS_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError(
        "Missing WITHINGS_CLIENT_ID or WITHINGS_CLIENT_SECRET in .env"
    )

_received = {"code": None, "error": None}
_server: HTTPServer | None = None


def exchange_code_for_tokens(code: str) -> dict:
    payload = {
        "action": "requesttoken",
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }
    r = requests.post(TOKEN_URL, data=payload, timeout=30)
    print("Token exchange HTTP:", r.status_code)
    print("Token exchange body (first 300 chars):", r.text[:300])

    r.raise_for_status()
    data = r.json()

    # Withings uses {"status":0,"body":{...}} for success
    if data.get("status") != 0:
        raise RuntimeError(f"Withings token exchange failed: {data}")

    return data["body"]


def update_env_refresh_token(new_refresh_token: str, env_path: str = ".env") -> None:
    # Read existing .env and update/insert WITHINGS_REFRESH_TOKEN
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()

    found = False
    out = []
    for line in lines:
        if line.startswith("WITHINGS_REFRESH_TOKEN="):
            out.append(f"WITHINGS_REFRESH_TOKEN={new_refresh_token}")
            found = True
        else:
            out.append(line)

    if not found:
        out.append(f"WITHINGS_REFRESH_TOKEN={new_refresh_token}")

    with open(env_path, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global _server
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        code = qs.get("code", [""])[0]
        error = qs.get("error", [""])[0]
        state = qs.get("state", [""])[0]

        # Basic validation
        if state and state != STATE:
            error = error or "state_mismatch"

        _received["code"] = code if code else None
        _received["error"] = error if error else None

        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()

        if _received["error"]:
            self.wfile.write(
                f"<h2>OAuth error</h2><p>{_received['error']}</p>".encode("utf-8")
            )
        else:
            self.wfile.write(
                b"<h2>LifeOps OAuth complete</h2><p>You can close this tab.</p>"
            )

        # Stop the server after handling the callback
        threading.Thread(target=_server.shutdown, daemon=True).start()

    def log_message(self, format, *args):
        # Silence default request logging
        return


def run_server():
    global _server
    host = "localhost"
    port = 8000
    _server = HTTPServer((host, port), Handler)
    print(f"Listening on {REDIRECT_URI} ...")
    _server.serve_forever()


def build_authorize_url() -> str:
    # Assemble without importing urlencode to keep it obvious
    return (
        f"{AUTHORIZE_URL}"
        f"?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&scope={SCOPE}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&state={STATE}"
    )


def main():
    # Start callback server in background thread
    t = threading.Thread(target=run_server, daemon=True)
    t.start()

    url = build_authorize_url()
    print("Opening browser for authorization:")
    print(url)
    webbrowser.open(url)

    # Wait for callback
    t.join()

    if _received["error"]:
        raise RuntimeError(f"OAuth callback error: {_received['error']}")

    if not _received["code"]:
        raise RuntimeError("No code received in callback.")

    print("Received auth code.")
    tokens = exchange_code_for_tokens(_received["code"])

    refresh_token = tokens.get("refresh_token")
    access_token = tokens.get("access_token")
    expires_in = tokens.get("expires_in")

    print("\n=== SUCCESS ===")
    print("expires_in:", expires_in)
    print("access_token present:", bool(access_token))
    print("refresh_token present:", bool(refresh_token))

    if not refresh_token:
        raise RuntimeError("No refresh_token returned by Withings.")

    update_env_refresh_token(refresh_token)
    print("\nUpdated .env with WITHINGS_REFRESH_TOKEN.")
    print("You can now move to the refresh-token automation + data pulls.")


if __name__ == "__main__":
    main()