#!/usr/bin/env python3
"""
Pre-flight freshness check — runs before the Monday pipeline.
Warns Shalini via Slack DM if any key data files haven't been
updated since last week, so stale data is never sent to the team.

Exit codes:
  0 = all files fresh, pipeline should proceed
  1 = stale files detected, warning sent (pipeline aborts)
"""

import os, sys, json, time, urllib.request, urllib.error
from datetime import datetime, timedelta

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR   = os.path.dirname(SCRIPT_DIR)
SLACK_TOKEN  = os.environ.get("SLACK_BOT_TOKEN", "")
SHALINI_ID   = "U08E254T9DW"

# Files that must be updated within the last 7 days
FILES_TO_CHECK = {
    "Priority accounts CSV": os.path.join(PARENT_DIR, "combined_high_priority_accounts_with_state.csv"),
    "Intent signals CSV":    os.path.join(PARENT_DIR, "high_priority_intent_engaged_overlap.csv"),
    "Hot this week JSON":    os.path.join(PARENT_DIR, "hot-this-week.json"),
    "APAC signals HTML":     os.path.join(PARENT_DIR, "apac-insights-hub", "index.html"),
}

STALE_THRESHOLD_DAYS = 7


def file_age_days(path):
    try:
        mtime = os.path.getmtime(path)
        age   = (time.time() - mtime) / 86400
        return round(age, 1)
    except FileNotFoundError:
        return None


def slack_dm(text, dry_run=False):
    if dry_run:
        print(text.encode("utf-8", errors="replace").decode("utf-8"))
        return
    if not SLACK_TOKEN:
        print("WARNING: SLACK_BOT_TOKEN not set — cannot send Slack warning.")
        return
    payload = json.dumps({"channel": SHALINI_ID, "text": text}).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={"Authorization": f"Bearer {SLACK_TOKEN}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if not data.get("ok"):
                print(f"Slack error: {data.get('error')}")
    except urllib.error.URLError as e:
        print(f"Request failed: {e}")


def main():
    dry_run = "--dry-run" in sys.argv
    today   = datetime.now().strftime("%A %d %B %Y")
    stale   = []
    missing = []

    print("Checking data freshness...")
    for label, path in FILES_TO_CHECK.items():
        age = file_age_days(path)
        if age is None:
            missing.append(label)
            print(f"  MISSING  {label}")
        elif age > STALE_THRESHOLD_DAYS:
            stale.append((label, age))
            print(f"  STALE    {label} — last updated {age}d ago")
        else:
            print(f"  OK       {label} — last updated {age}d ago")

    if not stale and not missing:
        print("\nAll files are fresh. Pipeline can proceed.")
        sys.exit(0)

    # Build warning message
    lines = [f"\u26a0\ufe0f *APAC Insights — stale data warning* ({today})\n"]
    lines.append("The Monday pipeline is *paused* because the following files look out of date:\n")

    for label, age in stale:
        lines.append(f"  \u2022 *{label}* \u2014 last updated *{age} days ago*")
    for label in missing:
        lines.append(f"  \u2022 *{label}* \u2014 file not found")

    lines.append("\n*To proceed:*")
    lines.append("  1. Drop the updated CSVs into `~/Cursor\\ Workspaces/outline/`")
    lines.append("  2. Regenerate and redeploy `apac-insights-hub/index.html`")
    lines.append("  3. Re-run the pipeline manually:\n     `cd ~/Cursor\\ Workspaces/outline/anz-sales-insights && ./deploy.sh`")
    lines.append("\nNo DMs or channel posts have been sent yet. \ud83d\udc4c")

    msg = "\n".join(lines)
    print(f"\n{msg}".encode("utf-8", errors="replace").decode("utf-8"))
    slack_dm(msg, dry_run=dry_run)

    sys.exit(1)


if __name__ == "__main__":
    main()
