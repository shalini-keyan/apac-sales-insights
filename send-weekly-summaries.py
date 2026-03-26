#!/usr/bin/env python3
"""
ANZ Sales Insights — Weekly Channel Summaries
Sends regional rollup posts to channels + leads every Monday.

Destinations:
  #anz-acquisition  → @James Johnson  (ANZ MM + LA)
  DM Ash Virgo                        (ANZ SMB)
  #revenue-apac     → @Vivey Wan      (GCR + ROA)
  #revenue-apac     → @Mitch Baba     (Japan)

Usage:
  python3 send-weekly-summaries.py              # send all
  python3 send-weekly-summaries.py --dry-run    # print without sending
  python3 send-weekly-summaries.py --region anz # send one region only
"""

import json, os, sys, argparse, urllib.request, urllib.error

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
SIGNALS_FILE = os.path.join(SCRIPT_DIR, "data", "signals.json")
REPS_FILE    = os.path.join(SCRIPT_DIR, "data", "reps.json")
HOT_FILE     = os.path.join(os.path.dirname(SCRIPT_DIR), "hot-this-week.json")
SITE_URL     = "https://apacinsights.quick.shopify.io"
SLACK_TOKEN  = os.environ.get("SLACK_BOT_TOKEN", "")

# ── Destinations ──────────────────────────────────────────────────────────────
DESTINATIONS = {
    "anz": {
        "target":    "C09FKQCPF42",   # #anz-acquisition
        "mention":   "<@U01VD2XDKT6>", # James Johnson
        "label":     "ANZ MM + LA",
        "aes":       ["Shane Kilgour", "Chachi Apolinario", "Lauren Critten",
                      "Kole Mahan", "Bronte Hogarth", "Amaly Khairallah",
                      "Karim Lalji", "Dugald Todd"],
    },
    "smb": {
        "target":    "U02P7JW8TGU",   # DM Ash Virgo
        "mention":   "Ash",
        "label":     "ANZ SMB",
        "aes":       ["Shane Kilgour", "Chachi Apolinario", "Lauren Critten",
                      "Kole Mahan", "Bronte Hogarth", "Amaly Khairallah",
                      "Karim Lalji", "Dugald Todd"],
    },
    "gcr": {
        "target":    "C01HD27JHR7",   # #revenue-apac
        "mention":   "<@U06HZEV9FUN>", # Vivey Wan
        "label":     "GCR + ROA",
        "aes":       ["Rae Chang", "Anwei Sun", "Sally Xin", "Nikhil Sareen"],
    },
    "japan": {
        "target":    "C01HD27JHR7",   # #revenue-apac
        "mention":   "<@U091L1203R7>", # Mitch Baba
        "label":     "Japan",
        "aes":       ["Eiji Hasegawa", "Jio Sotoyama", "Tanabe Rika",
                      "Yuki Kataoka", "Yuki Tokunaga"],
    },
}


def slack_post(target, text, dry_run=False):
    if dry_run:
        print(f"\n{'─'*60}")
        print(f"TO: {target}")
        print(text.encode("utf-8", errors="replace").decode("utf-8"))
        return True
    if not SLACK_TOKEN:
        print("ERROR: SLACK_BOT_TOKEN not set.")
        sys.exit(1)
    payload = json.dumps({"channel": target, "text": text}).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={"Authorization": f"Bearer {SLACK_TOKEN}", "Content-Type": "application/json"},
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


def get_pipeline_score(acc):
    for e in acc.get("engagement", []):
        if "Pipeline predict score:" in e:
            try:
                return int(e.replace("Pipeline predict score:", "").replace("%", "").strip())
            except ValueError:
                pass
    return 0


def classify_priority(acc, high_priority_names, signal_type):
    """
    For ANZ: use the CSV priority data.
    For other regions: use pipeline score + signal type as proxy.
      High   = MQA with score >= 65, or any score >= 75
      Medium = MQA with score 40-64, or score 40-74
      Low    = everything else
    """
    name = acc.get("account", "").lower()
    if high_priority_names is not None:
        if name in high_priority_names:
            return "high"
        else:
            return "low"
    # Fallback for non-ANZ regions
    score = get_pipeline_score(acc)
    if signal_type == "mqa":
        return "high"                        # all MQAs are high priority
    if signal_type == "lost":
        return "high"                        # reactivations are always high priority
    if score >= 65:
        return "high"
    if signal_type == "new" or score >= 40:
        return "medium"
    return "low"


def build_summary(dest_key, dest, signals_by_ae, high_priority_names, hot_ideas, week_label):
    mention = dest["mention"]
    label   = dest["label"]
    aes     = dest["aes"]
    hot_topic = hot_ideas[0]["topic"] if hot_ideas else None
    hot_why   = hot_ideas[0]["why_trending"] if hot_ideas else ""

    # Aggregate signals across all AEs in this region
    high_signals  = {"mqa": [], "lost": [], "new": [], "eng": []}
    other_counts  = {"mqa": 0, "lost": 0, "new": 0, "eng": 0}

    for ae in aes:
        buckets = signals_by_ae.get(ae, {})
        for stype in ["mqa", "lost", "new", "eng"]:
            for acc in buckets.get(stype, []):
                priority = classify_priority(acc, high_priority_names, stype)
                if priority == "high":
                    high_signals[stype].append({**acc, "ae": ae})
                else:
                    other_counts[stype] += 1

    total_high  = sum(len(v) for v in high_signals.values())
    total_other = sum(other_counts.values())

    # Signal summary line
    all_counts = {t: len(high_signals[t]) + other_counts[t] for t in ["mqa","lost","new","eng"]}
    parts = []
    if all_counts["mqa"]:  parts.append(f"*{all_counts['mqa']} MQA{'s' if all_counts['mqa']>1 else ''}*")
    if all_counts["lost"]: parts.append(f"*{all_counts['lost']} closed-lost reactivation{'s' if all_counts['lost']>1 else ''}*")
    if all_counts["new"]:  parts.append(f"*{all_counts['new']} new account{'s' if all_counts['new']>1 else ''}*")
    if all_counts["eng"]:  parts.append(f"*{all_counts['eng']} engaged contact{'s' if all_counts['eng']>1 else ''}*")
    summary_line = " · ".join(parts) if parts else "No new signals this week"

    # High priority accounts section
    high_lines = ""
    for stype, label_str in [("mqa","MQA"), ("lost","Reactivation"), ("new","New"), ("eng","Engaged")]:
        for acc in high_signals[stype][:3]:
            score = get_pipeline_score(acc)
            score_str = f" · {score}%" if score else ""
            note  = acc.get("note", "")
            ae_short = acc["ae"].split()[0]
            high_lines += f"\n  [{label_str}] *{acc['account']}* \u2014 {ae_short}{score_str}"
            if note:
                high_lines += f"\n    _{note}_"

    if not high_lines:
        high_lines = "\n  No high priority signals this week."

    # Medium/low summary
    other_parts = []
    if other_counts["mqa"]:  other_parts.append(f"{other_counts['mqa']} MQA{'s' if other_counts['mqa']>1 else ''}")
    if other_counts["lost"]: other_parts.append(f"{other_counts['lost']} reactivation{'s' if other_counts['lost']>1 else ''}")
    if other_counts["new"]:  other_parts.append(f"{other_counts['new']} new account{'s' if other_counts['new']>1 else ''}")
    if other_counts["eng"]:  other_parts.append(f"{other_counts['eng']} engaged contact{'s' if other_counts['eng']>1 else ''}")
    other_line = (", ".join(other_parts) + " across medium/low priority accounts \u2014 full details in each rep\u2019s DM.") if other_parts else ""

    msg = f"""Hey {mention} \ud83d\udc4b \u2014 *{label} weekly signals brief* for *{week_label}*

*This week:* {summary_line}

*High priority accounts to action:*{high_lines}"""

    if other_line:
        msg += f"\n\n*Also this week:* {other_line}"

    if hot_topic:
        msg += f"\n\n*\ud83d\udd25 Hot topic: {hot_topic}* \u2014 {hot_why[:120]}..."

    msg += f"\n\n\ud83d\udc49 *<{SITE_URL}|Full {label} dashboard>*\n\n_Sent by Pi \u00b7 ANZ Marketing_"

    return msg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--region",  help="Send one region only: anz, smb, gcr, japan")
    args = parser.parse_args()

    with open(SIGNALS_FILE) as f: signals_data = json.load(f)
    with open(REPS_FILE)    as f: reps_data    = json.load(f)
    with open(HOT_FILE)     as f: hot_data     = json.load(f)

    signals_by_ae = signals_data.get("by_ae", {})
    week_label    = hot_data.get("week", "this week")
    hot_ideas     = hot_data.get("ideas", [])

    # Build set of high priority ANZ account names (lowercase) from reps.json
    anz_high = set()
    for rep in reps_data["reps"]:
        if rep.get("region") == "ANZ":
            for acc in rep["accounts"]:
                if acc.get("priority") == "High":
                    anz_high.add(acc["name"].lower())

    # For non-ANZ regions, pass None so pipeline score is used instead
    priority_map = {
        "anz":   anz_high,
        "smb":   anz_high,
        "gcr":   None,
        "japan": None,
    }

    targets = [args.region] if args.region else list(DESTINATIONS.keys())

    print(f"Sending {'DRY RUN ' if args.dry_run else ''}weekly summaries ({', '.join(targets)})\n")

    for key in targets:
        dest = DESTINATIONS[key]
        msg  = build_summary(key, dest, signals_by_ae, priority_map[key], hot_ideas, week_label)
        label = f"{dest['label']} → {dest['target']}"
        if slack_post(dest["target"], msg, dry_run=args.dry_run):
            print(f"  ✓ {label}")
        else:
            print(f"  ✗ {label}")

    print(f"\n{'─'*60}\nDone.")


if __name__ == "__main__":
    main()
