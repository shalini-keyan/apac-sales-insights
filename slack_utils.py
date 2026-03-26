"""
Shared Slack posting utility.
Reads credentials from ~/.config/slack-mcp/credentials.json
(the same token used by the Slack MCP — no separate bot token needed).
"""

import json, os, urllib.request, urllib.error

CREDENTIALS_PATH = os.path.expanduser("~/.config/slack-mcp/credentials.json")


def _load_credentials():
    try:
        with open(CREDENTIALS_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        raise RuntimeError(
            f"Slack credentials not found at {CREDENTIALS_PATH}.\n"
            "Re-authenticate via Cursor: ask Pi to run update_auth."
        )


def slack_post(target, text, dry_run=False):
    if dry_run:
        print(f"\n{'─'*60}\nTO: {target}")
        print(text.encode("utf-8", errors="replace").decode("utf-8"))
        return True

    creds  = _load_credentials()
    token  = creds.get("token", "")
    cookie = creds.get("cookie", "")

    if not token:
        raise RuntimeError("No token found in Slack credentials file.")

    payload = json.dumps({"channel": target, "text": text}).encode()
    headers = {
        "Authorization":  f"Bearer {token}",
        "Content-Type":   "application/json",
    }
    if cookie:
        headers["Cookie"] = f"d={cookie}"

    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if not data.get("ok"):
                print(f"  Slack error: {data.get('error')}")
                return False
            return True
    except urllib.error.URLError as e:
        print(f"  Request failed: {e}")
        return False
