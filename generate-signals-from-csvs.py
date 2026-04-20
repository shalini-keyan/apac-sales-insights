#!/usr/bin/env python3
"""
Generate signals array from weekly CSVs → updates apac-sales-insights-hub.html

Run this each Monday after downloading the 4 CSV exports from Drive.

Usage:
  python3 generate-signals-from-csvs.py --csv-dir ~/Downloads --week "April 7, 2026"
  python3 generate-signals-from-csvs.py --csv-dir ~/Downloads --week "April 7, 2026" --dry-run

Open Opp Filtering
------------------
Any account with Open Pipeline > 0 (an active, non-closed opportunity in Salesforce)
is automatically excluded. Closed-lost accounts that are re-engaging are kept.
This check runs at generation time so open opps never appear in the hub or DMs.
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from collections import defaultdict

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR  = os.path.dirname(SCRIPT_DIR)
HUB_HTML    = os.path.join(PARENT_DIR, "apac-sales-insights-hub.html")
DEPLOY_HTML = os.path.join(PARENT_DIR, "apac-insights-hub", "index.html")

# ── CSV filename patterns (partial match, case-insensitive) ──────────────────
CSV_PATTERNS = {
    "new":  "apacbobnewlyengagedpeoplethisweek",
    "mqa":  "apacbobaccountswithhighintentandnosalestouches",
    "site": "apacbobwebsitevisitsintentsignalslast7days",
}


def find_csv(directory, pattern):
    """Find the most-recent CSV in directory whose lowercased name contains pattern.
    When multiple files match (e.g. file.csv, file (1).csv, file (2).csv) the one
    with the highest numeric suffix is preferred so we always use the latest export."""
    import re as _re
    matches = []
    for f in os.listdir(directory):
        if f.lower().endswith(".csv") and pattern in f.lower().replace(" ", "").replace("_", ""):
            # Extract trailing number for sorting; files without a number get -1
            m = _re.search(r"\((\d+)\)", f)
            num = int(m.group(1)) if m else -1
            matches.append((num, f))
    if not matches:
        return None
    matches.sort(key=lambda x: x[0], reverse=True)
    return os.path.join(directory, matches[0][1])


def read_csv(path):
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# ── AE → Segment mapping ────────────────────────────────────────────────────
# Update here when new AEs join or segments change.
AE_SEGMENTS = {
    # ANZ
    "Shane Kilgour":       "LA",
    "Lauren Critten":      "LA",
    "Kole Mahan":          "MM",
    "Chachi Apolinario":   "MM",
    "Bronte Hogarth":      "MM",
    "Morris Bray":         "SMB",
    "Amaly Khairallah":    "SMB",
    "Marty Nicholson":     "SMB",
    # Japan
    "Eiji Hasegawa":       "LA",
    "Jio Sotoyama":        "MM",
    "Yuki Tokunaga":       "MM",
    "Tanabe Rika":         "SMB",
    "Yuki Kataoka":        "SMB",
    # SEA / ROA / GCR
    "Karim Lalji":         ["LA", "MM"],
    "Rae Chang":           "LA",
    "Sally Xin":           "MM",
    "Weijie Neo":          "SMB",
    "Anwei Sun":           "SMB",
    # India
    "Nikhil Sareen":       ["LA", "MM", "SMB"],
}


def open_pipeline_value(row):
    """Return the Open Pipeline float for a row, or 0 if missing/empty."""
    raw = row.get("Open Pipeline", "") or row.get("open pipeline", "") or ""
    raw = raw.replace("$", "").replace(",", "").strip()
    try:
        return float(raw)
    except ValueError:
        return 0.0


# ── Country/Territory → Region mapping ─────────────────────────────────────
_TERRITORY_RE = re.compile(r"APAC_[^_]+_([A-Z]+)_", re.IGNORECASE)

_COUNTRY_TO_REGION = {
    # ANZ
    "australia": "ANZ", "au": "ANZ", "new zealand": "ANZ", "nz": "ANZ",
    "papua new guinea": "ANZ", "fiji": "ANZ", "samoa": "ANZ",
    "niue": "ANZ", "tuvalu": "ANZ", "cocos (keeling) islands": "ANZ",
    "french polynesia": "ANZ",
    # Japan
    "japan": "Japan",
    # GCR
    "china": "GCR", "hong kong": "GCR", "taiwan": "GCR", "macao": "GCR",
    "mongolia": "GCR",
    # India (South Asia)
    "india": "India", "in": "India", "bangladesh": "India",
    "nepal": "India", "pakistan": "India", "sri lanka": "India",
    "maldives": "India",
    # SEA / ROA
    "singapore": "SEA", "malaysia": "SEA", "indonesia": "SEA",
    "thailand": "SEA", "philippines": "SEA", "vietnam": "SEA",
    "myanmar": "SEA", "cambodia": "SEA", "brunei darussalam": "SEA",
    "korea, republic of": "SEA", "korea, democratic people's republic of": "SEA",
}

_TERRITORY_CODE_MAP = {
    "ANZ": "ANZ", "JPN": "Japan", "GCR": "GCR", "IND": "India",
    "ROA": "SEA", "SEA": "SEA",
}


def derive_region(row):
    """Derive hub region (ANZ/Japan/GCR/India/SEA) from Territory Name or Billing Country."""
    # 1. Try Territory Name — most reliable
    territory = row.get("Territory Name", "").strip()
    if territory:
        m = _TERRITORY_RE.search(territory)
        if m:
            code = m.group(1).upper()
            if code in _TERRITORY_CODE_MAP:
                return _TERRITORY_CODE_MAP[code]

    # 2. Fall back to Billing Country
    for col in ("Billing Country", "country", "acc_billingCountry___Demandbase"):
        country = row.get(col, "").strip().lower()
        if country and country in _COUNTRY_TO_REGION:
            return _COUNTRY_TO_REGION[country]

    # 3. Infer from AE name
    ae = row.get("Account Owner", "").strip()
    ae_region = {
        "Shane Kilgour": "ANZ", "Lauren Critten": "ANZ", "Kole Mahan": "ANZ",
        "Chachi Apolinario": "ANZ", "Bronte Hogarth": "ANZ", "Morris Bray": "ANZ",
        "Amaly Khairallah": "ANZ", "Marty Nicholson": "ANZ",
        "Eiji Hasegawa": "Japan", "Jio Sotoyama": "Japan", "Yuki Tokunaga": "Japan",
        "Tanabe Rika": "Japan", "Yuki Kataoka": "Japan",
        "Karim Lalji": "SEA", "Rae Chang": "GCR", "Sally Xin": "GCR",
        "Weijie Neo": "SEA", "Anwei Sun": "GCR",
        "Nikhil Sareen": "India",
    }
    return ae_region.get(ae, "")


def bq_open_opp_accounts(account_names):
    """
    Query BigQuery for any accounts (by name) with open (non-closed) opportunities.
    Returns a set of lowercase account names that have open opps.
    Falls back gracefully if bq CLI is unavailable or query fails.
    """
    if not account_names:
        return set()

    def esc(s):
        return s.replace("'", "\\'")

    quoted = ", ".join(f"'{esc(a)}'" for a in account_names)
    query = (
        "SELECT LOWER(a.name) AS account_name "
        "FROM `shopify-dw.sales.sales_opportunities_v1` o "
        "JOIN `shopify-dw.sales.crm_accounts` a ON o.salesforce_account_id = a.salesforce_id "
        f"WHERE LOWER(a.name) IN ({quoted}) AND o.is_closed = FALSE"
    )

    try:
        result = subprocess.run(
            ["bq", "query", "--nouse_legacy_sql", "--format=csv",
             "--project_id=shopify-dw", query],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            print(f"  [BQ] Warning: query failed — {result.stdout.strip() or result.stderr.strip()}")
            return set()
        lines = result.stdout.strip().splitlines()
        # First line is header "account_name"
        accounts_with_opps = {line.strip().lower() for line in lines[1:] if line.strip()}
        return accounts_with_opps
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        print(f"  [BQ] Warning: bq CLI unavailable ({e}) — skipping BigQuery opp check")
        return set()


def build_account_grade_lookup(new_rows):
    """Build account → grade dict from the Newly Engaged CSV (only CSV with Grade populated)."""
    lookup = {}
    for r in new_rows:
        acct  = r.get("Account Name", "").strip()
        grade = r.get("Account Grade", "").strip().upper()
        if acct and grade and acct not in lookup:
            lookup[acct] = grade
    return lookup


def build_new_signals(rows, excluded_accounts, worked_accounts=None, top_per_ae=20, min_grade="B"):
    """
    Type: new — net new accounts engaging for the first time, NOT yet being worked.
    - Grade A (high priority) and Grade B (medium priority) only (or Grade A only if min_grade="A").
    - Excludes accounts in worked_accounts (sales touches > 0 in site visits CSV).
    - Returns top_per_ae accounts per AE sorted by engagement (highest first).
    """
    if worked_accounts is None:
        worked_accounts = set()

    by_account = defaultdict(list)
    for r in rows:
        acct = r.get("Account Name", "").strip()
        if acct:
            by_account[acct].append(r)

    # Build candidate signals for all valid accounts
    candidates = []
    for acct, contacts in by_account.items():
        if acct.lower() in excluded_accounts:
            continue
        if acct.lower() in worked_accounts:
            continue  # already being worked by sales team
        rep = contacts[0]
        ae  = rep.get("Account Owner", "").strip()
        if ae not in AE_SEGMENTS:
            continue
        if open_pipeline_value(rep) > 0:
            continue

        # Grade filter
        grade = rep.get("Account Grade", "").strip().upper()
        valid_grades = {"A"} if min_grade == "A" else {"A", "B"}
        if grade and grade not in valid_grades:
            continue
        if min_grade == "A" and not grade:
            continue  # in high-priority mode, require explicit Grade A

        sdr  = rep.get("SDR Owner", rep.get("BDR Owner", "")).strip()
        ind  = rep.get("Industry", "").strip()
        reg  = derive_region(rep)
        web  = rep.get("Website", rep.get("Account Website", "")).strip()

        # Use Engagement Points (7 days) as the sort key
        eng_pts = max(
            float(r.get("Engagement Points (7 days)", r.get("Engagement Minutes", r.get("Engagement Score", 0))) or 0)
            for r in contacts
        )
        qual    = max(float(r.get("All Qualification Score", 0) or 0) for r in contacts)
        predict = max(float(r.get("All Pipeline Predict Score", 0) or 0) for r in contacts)
        kws_raw = next((r.get("High Intent Keywords","") for r in contacts if r.get("High Intent Keywords","")), "")
        keywords = [kw.strip().lower() for kw in kws_raw.split(",") if kw.strip()] if kws_raw else []

        contact_names = [
            r.get("Full Name", r.get("Contact Name", "")).strip()
            for r in contacts if r.get("Full Name") or r.get("Contact Name")
        ]
        contact_names = [n for n in contact_names if n]
        n_contacts = len(contact_names)

        if n_contacts > 1:
            note = f"{n_contacts} new contacts engaging for the first time this week. Engagement: {eng_pts:.0f} pts."
        elif contact_names:
            title = contacts[0].get("Title", contacts[0].get("Contact Title", "")).strip()
            name_str = contact_names[0]
            if title:
                name_str += f" ({title})"
            note = f"{name_str} engaging for the first time this week. Engagement: {eng_pts:.0f} pts."
        else:
            note = f"Account engaging for the first time this week. Engagement: {eng_pts:.0f} pts."
        if keywords:
            note += f" Intent: {', '.join(keywords[:3])}."

        engagement = []
        if eng_pts:
            engagement.append(f"Engagement score: {eng_pts:.0f} pts. (7 days)")
        if keywords:
            engagement.append(f"Searching for: {', '.join(keywords)}")
        if qual:
            engagement.append(f"Qualification score: {qual:.0%}")
        if predict:
            engagement.append(f"Pipeline predict score: {predict:.0%}")

        candidates.append({
            "_eng": eng_pts,
            "_ae":  ae,
            "type":      "new",
            "account":   acct,
            "website":   web,
            "ae":        ae,
            "sdr":       sdr,
            "industry":  ind,
            "region":    reg,
            "segment":   AE_SEGMENTS.get(ae, ""),
            "engagement": engagement,
            "note":      note,
            "days":      0,
            "open_opp":  False,
        })

    # Top N per AE by engagement score
    by_ae = defaultdict(list)
    for c in candidates:
        by_ae[c["_ae"]].append(c)
    signals = []
    for ae_candidates in by_ae.values():
        ae_candidates.sort(key=lambda x: x["_eng"], reverse=True)
        for s in ae_candidates[:top_per_ae]:
            del s["_eng"]; del s["_ae"]
            signals.append(s)
    return signals


def build_mqa_signals(rows, excluded_accounts, top_per_ae=10):
    """
    Type: mqa — hit MQA threshold with zero sales touches.
    Source: ApacBobAccountsWithHighIntentAndNoSalesTouches CSV.
    Excludes accounts with Open Pipeline > 0 (active open opportunity).
    Returns top_per_ae per AE sorted by 7-day engagement points.
    """
    from collections import defaultdict

    # Group by AE, dedup by account
    by_ae = defaultdict(list)
    seen = set()
    for r in rows:
        acct = r.get("Account Name", "").strip()
        if not acct or acct.lower() in excluded_accounts or acct in seen:
            continue

        op = open_pipeline_value(r)
        if op > 0:
            continue

        ae = r.get("Account Owner", "").strip()
        if ae not in AE_SEGMENTS:
            continue

        seen.add(acct)

        # Engagement score for ranking
        try:
            eng_score = float(r.get("Engagement Points (7 days)", "0") or "0")
        except ValueError:
            eng_score = 0.0

        sdr     = r.get("SDR Owner", r.get("BDR Owner", "")).strip()
        ind     = r.get("Industry", "").strip()
        reg     = derive_region(r)
        web     = r.get("Website", r.get("acc_website", "")).strip()

        # Pipeline predict score (stored as 0-1 decimal in this report)
        score_raw = r.get("All Pipeline Predict Score", r.get("Pipeline Predict Score", "")).strip()
        try:
            score_pct = f"{float(score_raw)*100:.0f}" if score_raw and float(score_raw) <= 1 else score_raw.replace("%", "")
        except ValueError:
            score_pct = ""

        # Intent keywords — column varies by report version
        intent_raw = (r.get("High Intent Keywords", "") or
                      r.get("Top Keywords", "") or
                      r.get("Intent Keywords", "") or
                      r.get("Keywords", "")).strip()
        intent_kws = ", ".join(k.strip().lower() for k in re.split(r"[;,]", intent_raw) if k.strip())[:80]

        engagement = []
        if score_pct:
            engagement.append(f"Pipeline predict score: {score_pct}%")
        if intent_kws:
            engagement.append(f"Searching for: {intent_kws}")

        note = "Hit MQA with zero sales touches."
        if intent_kws:
            kw_list = [k.strip() for k in intent_kws.split(",")]
            note = f"Hit MQA with zero sales touches. Primary intent: {kw_list[0]}."

        by_ae[ae].append({
            "_eng":      eng_score,
            "type":      "mqa",
            "account":   acct,
            "website":   web,
            "ae":        ae,
            "sdr":       sdr,
            "industry":  ind,
            "region":    reg,
            "segment":   AE_SEGMENTS.get(ae, ""),
            "engagement": engagement,
            "note":      note,
            "days":      0,
            "open_opp":  False,
        })

    signals = []
    for ae, candidates in by_ae.items():
        candidates.sort(key=lambda x: x["_eng"], reverse=True)
        for s in candidates[:top_per_ae]:
            del s["_eng"]
            signals.append(s)
    return signals


def build_site_signals(rows, excluded_accounts, grade_lookup=None, top_per_ae=20, min_grade="B"):
    """
    Type: eng only — high engagement site visits with zero sales touches.
    - Grade A/B accounts only (using grade_lookup from new contacts CSV).
    - Falls back to qualification score >= 0.5 when grade not available.
    - Accounts with touches > 0 (being worked) are excluded — team knows about these.
    - Accounts with Open Pipeline > 0 are excluded entirely.
    - Groups multiple page-visit rows per account into one signal.
    - Returns top_per_ae per AE sorted by 7-day engagement score.
    """
    if grade_lookup is None:
        grade_lookup = {}
    # Group all rows by account first
    by_account = defaultdict(list)
    for r in rows:
        acct = r.get("Account Name", "").strip()
        ae   = r.get("Account Owner", "").strip()
        if not acct or acct.lower() in excluded_accounts:
            continue
        if ae not in AE_SEGMENTS:
            continue
        if open_pipeline_value(r) > 0:
            continue
        by_account[acct].append(r)

    eng_candidates = []

    for acct, acct_rows in by_account.items():
        rep     = acct_rows[0]
        ae      = rep.get("Account Owner", "").strip()
        sdr     = rep.get("SDR Owner", rep.get("BDR Owner", "")).strip()
        ind     = rep.get("Industry", "").strip()
        reg     = derive_region(rep)
        web     = rep.get("Website", rep.get("Account Website", "")).strip()
        touches = int(rep.get("Sales Touches (14 days)", 0) or 0)
        stage   = rep.get("Journey Stage", rep.get("Account Stage", "")).strip().lower()

        # Aggregate across all rows for this account
        eng_7d  = max(float(r.get("Engagement Points (7 days)", 0) or 0) for r in acct_rows)
        eng_3m  = max(float(r.get("Engagement Points (3 mo.)", 0) or 0) for r in acct_rows)
        qual    = max(float(r.get("All Qualification Score", 0) or 0) for r in acct_rows)
        predict = max(float(r.get("All Pipeline Predict Score", 0) or 0) for r in acct_rows)

        pages = sorted(set(
            r.get("Visited Web Page", "").strip()
            for r in acct_rows if r.get("Visited Web Page", "").strip()
        ))
        keywords = sorted(set(
            kw.strip().lower()
            for r in acct_rows
            for kw in (r.get("High Intent Keywords", "") or "").split(",")
            if kw.strip()
        ))

        engagement = []
        if eng_7d:
            eng_str = f"Engagement score: {eng_7d:.0f} pts. (7 days)"
            if eng_3m: eng_str += f" · {eng_3m:.0f} pts. (3 months)"
            engagement.append(eng_str)
        if pages:
            engagement.append(f"Pages visited: {', '.join(pages)}")
        if keywords:
            engagement.append(f"Searching for: {', '.join(keywords)}")
        if qual:
            engagement.append(f"Qualification score: {qual:.0%}")
        if predict:
            engagement.append(f"Pipeline predict score: {predict:.0%}")

        note_parts = [f"{len(pages)} Shopify page(s) visited this week."]
        if touches:
            note_parts.append(f"{touches} sales touch(es) in last 14 days.")
        if keywords:
            note_parts.append(f"Intent: {', '.join(keywords[:3])}.")
        note = " ".join(note_parts)

        # Skip accounts already being worked by the sales team
        if touches > 0:
            continue

        # Grade/priority filter — cross-ref grade_lookup from new contacts CSV
        grade = grade_lookup.get(acct, "").upper()
        if min_grade == "A":
            if grade != "A":
                continue  # high-priority mode: Grade A only
        else:
            if grade in ("C", "D"):
                continue
            if not grade and qual < 0.5:
                continue


        eng_candidates.append({
            "_eng": eng_7d,
            "_ae":  ae,
            "type":       "eng",
            "account":    acct,
            "website":    web,
            "ae":         ae,
            "sdr":        sdr,
            "industry":   ind,
            "region":     reg,
            "segment":    AE_SEGMENTS.get(ae, ""),
            "engagement": engagement,
            "note":       note,
            "days":       0,
            "open_opp":   False,
        })

    # Cap to top N per AE by 7-day engagement score
    by_ae = defaultdict(list)
    for s in eng_candidates:
        by_ae[s["_ae"]].append(s)
    eng_signals = []
    for ae_sigs in by_ae.values():
        ae_sigs.sort(key=lambda x: x["_eng"], reverse=True)
        for s in ae_sigs[:top_per_ae]:
            del s["_eng"]; del s["_ae"]
            eng_signals.append(s)

    return eng_signals


def inject_signals_into_html(html_path, signals, week_label):
    with open(html_path) as f:
        html = f.read()

    # Update WEEK constant
    html = re.sub(r'const WEEK\s*=\s*"[^"]*"', f'const WEEK = "{week_label}"', html)

    # Replace signals array
    match = re.search(r'const signals = (\[)', html, re.DOTALL)
    if not match:
        raise ValueError("Could not find 'const signals = [' in HTML")

    pos = match.start(1)
    depth = 0
    i = pos
    while i < len(html):
        if html[i] == "[":
            depth += 1
        elif html[i] == "]":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
        i += 1

    new_array = json.dumps(signals, indent=2, ensure_ascii=False)
    html = html[:pos] + new_array + html[end:]

    with open(html_path, "w") as f:
        f.write(html)


def main():
    parser = argparse.ArgumentParser(description="Generate APAC Insights Hub signals from CSVs")
    parser.add_argument("--csv-dir", default=os.path.expanduser("~/Downloads"),
                        help="Directory containing the weekly CSV exports")
    parser.add_argument("--week", required=True,
                        help='Week label, e.g. "April 7, 2026"')
    parser.add_argument("--dry-run", action="store_true",
                        help="Print summary without writing any files")
    parser.add_argument("--top-per-ae", type=int, default=20,
                        help="Max signals per AE for new/eng types (default: 20)")
    parser.add_argument("--new-csv",  help="Explicit path to NewlyEngaged CSV")
    parser.add_argument("--mqa-csv",  help="Explicit path to HighIntentNoSalesTouches CSV")
    parser.add_argument("--site-csv", help="Explicit path to WebsiteVisits CSV")
    parser.add_argument("--net-new", metavar="PREV_ACCOUNTS_JSON",
                        help="Path to JSON file of last week's account names (lowercase). "
                             "Excludes any account already seen last week.")
    parser.add_argument("--high-priority", action="store_true",
                        help="Only include Grade A accounts (skips Grade B and ungraded).")
    args = parser.parse_args()

    csv_dir = args.csv_dir

    # ── Find CSVs ──────────────────────────────────────────────────────────
    new_csv  = args.new_csv  or find_csv(csv_dir, CSV_PATTERNS["new"])
    mqa_csv  = args.mqa_csv  or find_csv(csv_dir, CSV_PATTERNS["mqa"])
    site_csv = args.site_csv or find_csv(csv_dir, CSV_PATTERNS["site"])

    missing = []
    if not new_csv:  missing.append("NewlyEngaged")
    if not mqa_csv:  missing.append("HighIntentNoSalesTouches")
    if not site_csv: missing.append("WebsiteVisitsIntentSignals")

    if missing:
        print(f"ERROR: Could not find CSVs for: {', '.join(missing)}")
        print(f"  Looking in: {csv_dir}")
        sys.exit(1)

    print(f"CSVs found:")
    print(f"  new:  {os.path.basename(new_csv)}")
    print(f"  mqa:  {os.path.basename(mqa_csv)}")
    print(f"  site: {os.path.basename(site_csv)}")

    new_rows  = read_csv(new_csv)
    mqa_rows  = read_csv(mqa_csv)
    site_rows = read_csv(site_csv)

    # ── Build excluded accounts set (open pipeline > 0) ───────────────────
    excluded = set()
    for r in mqa_rows:
        if open_pipeline_value(r) > 0:
            excluded.add(r.get("Account Name", "").strip().lower())
    for r in site_rows:
        if open_pipeline_value(r) > 0:
            excluded.add(r.get("Account Name", "").strip().lower())
    for r in new_rows:
        if open_pipeline_value(r) > 0:
            excluded.add(r.get("Account Name", "").strip().lower())

    print(f"\nAccounts with open pipeline (excluded via CSV): {len(excluded)}")

    # ── BigQuery open-opp cross-check ─────────────────────────────────────
    # Collect all candidate account names across the three CSVs
    all_candidate_names = set()
    for r in new_rows + mqa_rows + site_rows:
        acct = r.get("Account Name", "").strip()
        if acct and acct.lower() not in excluded:
            all_candidate_names.add(acct.lower())

    print(f"Running BigQuery open-opp check on {len(all_candidate_names)} candidate accounts...")
    bq_open_accts = bq_open_opp_accounts(all_candidate_names)
    if bq_open_accts:
        print(f"  [BQ] Found {len(bq_open_accts)} accounts with open opps — excluding:")
        for name in sorted(bq_open_accts):
            print(f"    • {name}")
        excluded |= bq_open_accts
    else:
        print("  [BQ] No open opps found in candidate accounts.")

    # ── Net-new filter: exclude accounts from previous week ────────────────
    if args.net_new:
        with open(args.net_new) as f:
            prev_accounts = set(json.load(f))
        before = len(excluded)
        excluded |= prev_accounts
        print(f"Net-new filter: excluded {len(prev_accounts)} prev-week accounts "
              f"({len(excluded)-before} net new exclusions added)")

    # ── High-priority filter: Grade A only ────────────────────────────────
    min_grade = "A" if args.high_priority else "B"
    if args.high_priority:
        print("High-priority mode: Grade A accounts only")

    # ── Generate signals ────────────────────────────────────────────────────
    # Build grade lookup (only available in new contacts CSV)
    grade_lookup = build_account_grade_lookup(new_rows)

    # Build worked accounts set (touches > 0 in site visits CSV) to exclude from new signals
    worked_accounts = set()
    for r in site_rows:
        touches = int(r.get("Sales Touches (14 days)", 0) or 0)
        if touches > 0:
            worked_accounts.add(r.get("Account Name", "").strip().lower())

    new_signals = build_new_signals(new_rows, excluded, worked_accounts=worked_accounts,
                                    top_per_ae=args.top_per_ae, min_grade=min_grade)
    mqa_signals = build_mqa_signals(mqa_rows, excluded, top_per_ae=5)
    eng_sig     = build_site_signals(site_rows, excluded, grade_lookup=grade_lookup,
                                     top_per_ae=args.top_per_ae, min_grade=min_grade)

    # Order: new first (newly engaged), then mqa, then engagement
    all_signals = new_signals + mqa_signals + eng_sig

    # Assign sequential IDs
    for i, s in enumerate(all_signals, 1):
        s["id"] = i

    removed_count = sum(
        1 for rows in [mqa_rows, site_rows, new_rows]
        for r in rows
        if r.get("Account Name", "").strip().lower() in excluded
    )

    print(f"\nSignals generated: {len(all_signals)}")
    print(f"  {len(new_signals)} new accounts (Grade A/B, not yet worked)")
    print(f"  {len(mqa_signals)} MQAs")
    print(f"  {len(eng_sig)} engaged contacts (Grade A/B or qual ≥50%, no rep touch)")
    print(f"  {len(worked_accounts)} accounts with rep touches excluded (being worked)")
    print(f"  (excluded ~{len(excluded)} accounts with open pipeline)")

    if args.dry_run:
        print("\n[dry-run] No files written.")
        return

    # ── Write to hub HTML and deploy copy ──────────────────────────────────
    inject_signals_into_html(HUB_HTML, all_signals, args.week)
    print(f"\nUpdated: {HUB_HTML}")

    os.makedirs(os.path.dirname(DEPLOY_HTML), exist_ok=True)
    import shutil
    shutil.copy(HUB_HTML, DEPLOY_HTML)
    print(f"Copied to: {DEPLOY_HTML}")

    # Save this week's accounts for next week's net-new filter
    this_week_accounts = sorted({s["account"].lower() for s in all_signals})
    accounts_file = os.path.join(os.path.dirname(__file__), "last-week-accounts.json")
    with open(accounts_file, "w") as f:
        json.dump(this_week_accounts, f, indent=2)
    print(f"Saved {len(this_week_accounts)} accounts for next week's net-new filter → {accounts_file}")

    print(f"\nNext: deploy with `quick deploy apac-insights-hub apacinsights --force`")


if __name__ == "__main__":
    main()
