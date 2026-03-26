#!/usr/bin/env python3
"""
ANZ Sales Insights — Weekly Slack DM Sender
Reads reps.json + hot-this-week.json and sends personalised Monday morning DMs.

Usage:
  python3 send-weekly-dms.py              # send to all reps
  python3 send-weekly-dms.py --dry-run    # print messages without sending
  python3 send-weekly-dms.py --rep lauren_critten   # send to one rep only
"""

import json
import os
import sys
import argparse
import urllib.request
import urllib.error

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
REPS_FILE    = os.path.join(SCRIPT_DIR, "data", "reps.json")
SIGNALS_FILE = os.path.join(SCRIPT_DIR, "data", "signals.json")
HOT_FILE     = os.path.join(os.path.dirname(SCRIPT_DIR), "hot-this-week.json")
SITE_URL    = "https://apacinsights.quick.shopify.io"

SLACK_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")


def slack_post(channel, text, dry_run=False):
    if dry_run:
        print(f"\n{'─'*60}")
        print(f"TO: {channel}")
        print(text)
        return True

    if not SLACK_TOKEN:
        print("ERROR: SLACK_BOT_TOKEN environment variable not set.")
        sys.exit(1)

    payload = json.dumps({"channel": channel, "text": text}).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Authorization": f"Bearer {SLACK_TOKEN}",
            "Content-Type": "application/json",
        },
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


def build_message(rep, signals_by_ae, hot_ideas, week_label):
    name_first = rep["name"].split()[0]
    region = rep.get("region", "APAC")
    dashboard_url = f"{SITE_URL}/?seller={rep['slug']}"

    buckets  = signals_by_ae.get(rep["name"], {})
    mqas     = buckets.get("mqa",  [])
    lost     = buckets.get("lost", [])
    new_accs = buckets.get("new",  [])
    eng      = buckets.get("eng",  [])
    total    = len(mqas) + len(lost) + len(new_accs) + len(eng)

    hot_topic = hot_ideas[0]["topic"] if hot_ideas else None
    hot_why   = hot_ideas[0]["why_trending"] if hot_ideas else ""

    # Signal summary line
    parts = []
    if mqas:     parts.append(f"*{len(mqas)} MQA{'s' if len(mqas)>1 else ''}*")
    if lost:     parts.append(f"*{len(lost)} closed-lost reactivation{'s' if len(lost)>1 else ''}*")
    if new_accs: parts.append(f"*{len(new_accs)} new account{'s' if len(new_accs)>1 else ''}*")
    if eng:      parts.append(f"*{len(eng)} engaged contact{'s' if len(eng)>1 else ''}*")
    summary = " · ".join(parts) if parts else "No new signals this week"

    # Top accounts to highlight (MQAs first, then lost, then new)
    highlights = (mqas + lost + new_accs)[:4]
    acc_lines = ""
    for a in highlights:
        note = a.get("note", "")
        score_match = next((e for e in a.get("engagement", []) if "Pipeline predict" in e), "")
        score = score_match.replace("Pipeline predict score: ", "").strip() if score_match else ""
        score_str = f" · {score}" if score else ""
        acc_lines += f"\n  • *{a['account']}* ({a['industry']}){score_str}"
        if note:
            acc_lines += f"\n    _{note}_"

    if not acc_lines:
        acc_lines = "\n  No new signals this week — check back next Monday."

    message = f"""👋 Hey {name_first} — here's your {region} insights brief for *{week_label}*

*This week's signals:* {summary}

*Accounts to prioritise:*{acc_lines}"""

    if hot_topic:
        message += f"""

*🔥 Hot topic across APAC this week: {hot_topic}*
_{hot_why}_"""

    message += f"""

👉 *<{dashboard_url}|Open your full dashboard>*

_Sent by Pi · ANZ Marketing · {SITE_URL}_"""

    return message


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print messages without sending")
    parser.add_argument("--rep",     help="Slug of a single rep to send to (e.g. lauren_critten)")
    args = parser.parse_args()

    with open(REPS_FILE)    as f: reps_data    = json.load(f)
    with open(SIGNALS_FILE) as f: signals_data = json.load(f)
    with open(HOT_FILE)     as f: hot_data     = json.load(f)

    signals_by_ae = signals_data.get("by_ae", {})

    week_label = hot_data.get("week", "this week")
    hot_ideas  = hot_data.get("ideas", [])
    reps       = reps_data["reps"]

    if args.rep:
        reps = [r for r in reps if r["slug"] == args.rep]
        if not reps:
            print(f"Rep '{args.rep}' not found. Available: {[r['slug'] for r in reps_data['reps']]}")
            sys.exit(1)

    print(f"Sending {'DRY RUN ' if args.dry_run else ''}weekly briefs for {week_label}")
    print(f"Sending to {len(reps)} rep(s)…\n")

    ok = 0
    for rep in reps:
        msg = build_message(rep, signals_by_ae, hot_ideas, week_label)
        label = f"{rep['name']} ({rep['slack_id']})"
        if slack_post(rep["slack_id"], msg, dry_run=args.dry_run):
            print(f"  ✓ {label}")
            ok += 1
        else:
            print(f"  ✗ {label}")

    print(f"\n{'─'*60}")
    print(f"Done: {ok}/{len(reps)} sent successfully.")


if __name__ == "__main__":
    main()
