#!/usr/bin/env python3
"""
ANZ Sales Insights — Rep Data Generator
Processes priority accounts + intent signals into per-rep JSON for the quick site.
Run this weekly before deploying.
"""

import csv
import json
import os
from collections import defaultdict
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)

ACCOUNTS_CSV = os.path.join(PARENT_DIR, "combined_high_priority_accounts_with_state.csv")
INTENT_CSV = os.path.join(PARENT_DIR, "high_priority_intent_engaged_overlap.csv")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "data", "reps.json")

REP_SLUGS = {
    "Lauren Critten":    {"slug": "lauren_critten",    "slack_id": "U0A6QMG9G6A"},
    "Shane Kilgour":     {"slug": "shane_kilgour",     "slack_id": "W018304GAFR"},
    "Chachi Apolinario": {"slug": "chachi_apolinario", "slack_id": "U02EF43RWPK"},
    "Kole Mahan":        {"slug": "kole_mahan",        "slack_id": "U08L5ALENDP"},
    "Bronte Hogarth":    {"slug": "bronte_hogarth",    "slack_id": "W018HNGSV42"},
    "Dugald Todd":       {"slug": "dugald_todd",       "slack_id": "U02E6T5QS72"},
}

INTENT_TOPIC_LABELS = {
    "AI related keywords":          "AI / Agentic Commerce",
    "Growth Campaign Keyword Set":  "Growth",
    "Plus General Set":             "Plus",
    "POS General & Competitors":    "POS / Competitive",
    "CMO Set":                      "Marketing / CMO",
    "APAC LA Keywords":             "APAC LA",
    "www.shopify.com":              "Shopify.com Visit",
}

def label_intent(raw):
    for key, label in INTENT_TOPIC_LABELS.items():
        if key.lower() in raw.lower():
            return label
    return raw.strip()


def load_intent():
    intent_map = {}
    with open(INTENT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row["Account Name"].strip()
            domain = row["Domain/Website"].strip().lstrip("http://").lstrip("https://")
            topics = set()
            raw_category = row.get("Sample Activity Category", "")
            raw_details = row.get("Sample Activity Details", "")
            for raw in [raw_category, raw_details]:
                for part in raw.split(","):
                    part = part.strip()
                    if part:
                        topics.add(label_intent(part))
            intent_map[name.lower()] = {
                "account_id":     row.get("Account ID", ""),
                "domain":         domain,
                "owner":          row["Account Owner"].strip(),
                "city":           row.get("Billing City", "").strip(),
                "state":          row.get("Billing State/Province", "").strip(),
                "activity_count": int(row.get("Activity Count (30d)", 0) or 0),
                "latest_activity":row.get("Latest Activity Date", ""),
                "intent_topics":  sorted(topics),
            }
    return intent_map


def load_accounts():
    reps = defaultdict(list)
    with open(ACCOUNTS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            owner = row["Account Owner"].strip()
            if owner not in REP_SLUGS:
                continue
            reps[owner].append({
                "name":     row["Account Name"].strip(),
                "website":  row["Website"].strip(),
                "industry": row["Industry"].strip(),
                "priority": row["Priority"].strip(),
                "state":    row.get("Billing State", "").strip(),
                "bob":      row.get("BoB Source", "").strip(),
            })
    return reps


def build_reps_json(accounts, intent_map):
    result = {
        "generated": datetime.utcnow().isoformat() + "Z",
        "site_url":  "https://apacinsights.quick.shopify.io",
        "reps": []
    }

    for rep_name, info in REP_SLUGS.items():
        accs = accounts.get(rep_name, [])
        enriched = []
        intent_count = 0

        for acc in accs:
            key = acc["name"].lower()
            intent = intent_map.get(key, {})
            has_intent = bool(intent)
            if has_intent:
                intent_count += 1
            enriched.append({
                **acc,
                "has_intent":     has_intent,
                "activity_count": intent.get("activity_count", 0),
                "intent_topics":  intent.get("intent_topics", []),
                "latest_activity":intent.get("latest_activity", ""),
                "city":           intent.get("city", acc.get("state", "")),
                "account_id":     intent.get("account_id", ""),
            })

        enriched.sort(key=lambda x: (-x["activity_count"], x["name"]))

        high_priority    = [a for a in enriched if a["priority"] == "High"]
        with_intent      = [a for a in enriched if a["has_intent"]]
        top_intent_topics = {}
        for a in with_intent:
            for t in a["intent_topics"]:
                top_intent_topics[t] = top_intent_topics.get(t, 0) + 1
        top_topics = sorted(top_intent_topics.items(), key=lambda x: -x[1])[:5]

        result["reps"].append({
            "name":            rep_name,
            "slug":            info["slug"],
            "slack_id":        info["slack_id"],
            "total_accounts":  len(accs),
            "high_priority":   len(high_priority),
            "intent_count":    intent_count,
            "top_intent_topics": [{"topic": t, "count": c} for t, c in top_topics],
            "accounts":        enriched,
        })

    result["reps"].sort(key=lambda r: -r["intent_count"])
    return result


def main():
    print("Loading intent signals...")
    intent_map = load_intent()
    print(f"  {len(intent_map)} accounts with intent")

    print("Loading priority accounts...")
    accounts = load_accounts()
    total = sum(len(v) for v in accounts.values())
    print(f"  {total} accounts across {len(accounts)} reps")

    print("Building reps.json...")
    data = build_reps_json(accounts, intent_map)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nDone → {OUTPUT_FILE}")
    for rep in data["reps"]:
        print(f"  {rep['name']}: {rep['total_accounts']} accounts, {rep['intent_count']} with intent")


if __name__ == "__main__":
    main()
