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
REPS_FILE   = os.path.join(SCRIPT_DIR, "data", "reps.json")
HOT_FILE    = os.path.join(os.path.dirname(SCRIPT_DIR), "hot-this-week.json")
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


def build_message(rep, hot_ideas, week_label):
    name_first = rep["name"].split()[0]
    dashboard_url = f"{SITE_URL}/?seller={rep['slug']}"

    intent_accs = [a for a in rep["accounts"] if a["has_intent"]]
    top_intent  = sorted(intent_accs, key=lambda x: -x["activity_count"])[:5]

    # Top intent topic
    top_topics = rep.get("top_intent_topics", [])
    top_topic_str = top_topics[0]["topic"] if top_topics else "—"

    # Hot this week — top topic
    hot_topic = hot_ideas[0]["topic"] if hot_ideas else None
    hot_why   = hot_ideas[0]["why_trending"] if hot_ideas else ""

    # Account list
    acc_lines = ""
    for a in top_intent:
        topics = ", ".join(a["intent_topics"][:2]) if a["intent_topics"] else "general intent"
        acc_lines += f"\n  • *{a['name']}* — {a['activity_count']} signals · _{topics}_"

    if not acc_lines:
        acc_lines = "\n  No new intent signals this week."

    message = f"""👋 Hey {name_first} — here's your ANZ insights brief for *{week_label}*

*Your BoB at a glance:*
• {rep['total_accounts']} accounts in book · {rep['intent_count']} showing active intent · {rep['high_priority']} high priority
• Top intent theme this week: *{top_topic_str}*

*Accounts with the most signals:*{acc_lines}"""

    if hot_topic:
        message += f"""

*🔥 Hot topic across ANZ this week: {hot_topic}*
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

    with open(REPS_FILE)  as f: reps_data = json.load(f)
    with open(HOT_FILE)   as f: hot_data  = json.load(f)

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
        msg = build_message(rep, hot_ideas, week_label)
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
