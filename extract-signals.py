#!/usr/bin/env python3
"""
Extract signals from apac-insights-hub/index.html → data/signals.json
Run this whenever the old site HTML is updated with fresh data.
"""

import re, json, os
from collections import defaultdict

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
SOURCE_HTML = os.path.join(os.path.dirname(SCRIPT_DIR), "apac-insights-hub", "index.html")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "data", "signals.json")

TYPE_LABELS = {
    "mqa":  "MQA",
    "lost": "Closed-Lost Reactivation",
    "new":  "New Account",
    "eng":  "Engaged Contact",
    "unm":  "Unmanaged",
}

def main():
    with open(SOURCE_HTML) as f:
        html = f.read()

    match = re.search(r'const signals = (\[.*?\]);', html, re.DOTALL)
    if not match:
        print("ERROR: Could not find signals array in HTML.")
        return

    signals = json.loads(match.group(1))
    print(f"Extracted {len(signals)} signals")

    by_ae = defaultdict(lambda: {"mqa": [], "lost": [], "new": [], "eng": [], "unm": []})
    for s in signals:
        ae   = s.get("ae", "Unknown")
        stype = s.get("type", "unm")
        by_ae[ae][stype].append({
            "account":    s.get("account", ""),
            "website":    s.get("website", ""),
            "industry":   s.get("industry", ""),
            "region":     s.get("region", ""),
            "engagement": s.get("engagement", []),
            "note":       s.get("note", ""),
            "days":       s.get("days", 0),
            "id":         s.get("id", 0),
        })

    result = {
        "total": len(signals),
        "by_ae": {ae: dict(buckets) for ae, buckets in by_ae.items()},
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nDone → {OUTPUT_FILE}")
    for ae, buckets in sorted(by_ae.items(), key=lambda x: -sum(len(v) for v in x[1].values())):
        total = sum(len(v) for v in buckets.values())
        breakdown = ", ".join(f"{len(v)} {k}" for k, v in buckets.items() if v)
        print(f"  {ae}: {total} signals ({breakdown})")

if __name__ == "__main__":
    main()
