"""
Phase 1 - one-off backfill of APN + MLS data for rows already in Supabase
--------------------------------------------------------------------------
Run locally from this folder (reads .env the same way backfill.py does).
Run migrations/001_apn_and_mls.sql in Supabase FIRST.

EVERY STEP IS A DRY RUN UNLESS YOU PASS --commit.

Recommended order:

  1. python phase1_backfill.py mls-test --apn 157-791-64-00
       One MLS query, prints the raw response. Confirms auth, that
       ParcelNumber search works, and that Closed records come back.
       Writes nothing, costs nothing on PropertyRadar.

  2. python phase1_backfill.py apn-csv --csv Export-20260730-122559.csv --csv Export-20260730-163222.csv
     python phase1_backfill.py apn-csv --csv ... --csv ... --commit
       FREE. Fills `apn` (and `property_type` where it's null) from
       PropertyRadar exports you already have. Only touches rows that
       already exist in Supabase - never inserts.

  3. python phase1_backfill.py apn-pr --probe
       Makes ONE PropertyRadar call with Purchase=0 and prints the
       response, so you can confirm the APN field name/cost before paying.
     python phase1_backfill.py apn-pr
       Dry run: lists which rows still lack an APN and would be charged.
     python phase1_backfill.py apn-pr --commit [--limit 10]
       PAID. This is the deliberate safeguard override: these RadarIDs are
       already in processed_radar_ids, so the webhook would never re-pull
       them. Every paid result is appended to pr_apn_results.jsonl BEFORE
       the Supabase write; any RadarID in that file is skipped on re-runs,
       so a crash or re-run can never charge twice for the same property.
       The webhook's own ledger check is untouched.

  4. python phase1_backfill.py mls --limit 5            (dry run, prints results)
     python phase1_backfill.py mls --commit             (all rows not yet checked)
     python phase1_backfill.py mls --commit --all       (re-check everything)
     python phase1_backfill.py mls --commit --radar-id P108C87F
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime

import requests
from dotenv import load_dotenv
from supabase import create_client

from mls import RealtyfeedClient, enrich_property

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
PROPERTYRADAR_API_KEY = os.environ.get("PROPERTYRADAR_API_KEY", "")
PR_BASE_URL = "https://api.propertyradar.com/v1/properties"
PR_LOG = "pr_apn_results.jsonl"

PTYPE_TO_ABBREVIATION = {"Single Family": "SFR", "Condominium": "CND"}  # same map as main.py


def get_supabase():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        sys.exit("ERROR: SUPABASE_URL / SUPABASE_SERVICE_KEY not found in .env")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def fetch_all(sb, columns, apply_filters=None, page=1000):
    """Supabase returns at most 1000 rows per request - page through all of them."""
    rows, start = [], 0
    while True:
        q = sb.table("properties").select(columns)
        if apply_filters:
            q = apply_filters(q)
        batch = q.order("radar_id").range(start, start + page - 1).execute().data or []
        rows.extend(batch)
        if len(batch) < page:
            return rows
        start += page


def banner(commit):
    print("=" * 60)
    print("MODE: COMMIT (writes / charges are real)" if commit else "MODE: DRY RUN (nothing written, nothing charged)")
    print("=" * 60)


# ============================================================
# 1. mls-test
# ============================================================

def cmd_mls_test(args):
    client = RealtyfeedClient()
    if not client.configured:
        sys.exit("ERROR: REALTYFEED_CLIENT_ID / REALTYFEED_CLIENT_SECRET not set in .env")
    results = client.listings_for_apn(args.apn)
    print(f"{len(results)} MLS record(s) for APN {args.apn}:\n")
    print(json.dumps(results, indent=2, default=str))


# ============================================================
# 2. apn-csv (free)
# ============================================================

def cmd_apn_csv(args):
    sb = get_supabase()
    banner(args.commit)

    from_csv = {}
    for path in args.csv:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                rid = (r.get("Radar ID") or "").strip()
                if rid and (r.get("APN") or "").strip():
                    from_csv[rid] = {"apn": r["APN"].strip(), "type": (r.get("Type") or "").strip() or None}
    print(f"APNs available from CSVs: {len(from_csv)}")

    existing = fetch_all(sb, "radar_id, apn, property_type")
    print(f"Rows in Supabase: {len(existing)}")

    apn_updates, type_updates = [], []
    for row in existing:
        src = from_csv.get(row["radar_id"])
        if not src:
            continue
        if not row.get("apn") or args.overwrite:
            apn_updates.append({"radar_id": row["radar_id"], "apn": src["apn"]})
        if not row.get("property_type") and src["type"]:
            type_updates.append({"radar_id": row["radar_id"], "property_type": src["type"]})

    still_missing = sum(1 for r in existing if not r.get("apn") and r["radar_id"] not in from_csv)
    print(f"Will set APN on:            {len(apn_updates)} rows")
    print(f"Will fill null property_type: {len(type_updates)} rows")
    print(f"Still missing APN after this: {still_missing} rows  (-> apn-pr step)")

    if not args.commit:
        print("\nDry run - re-run with --commit to write.")
        return

    # Only radar_ids that already exist are in these lists, so upsert can
    # only UPDATE - it can never insert a new row. Upsert touches only the
    # columns present in the payload.
    for label, updates in (("apn", apn_updates), ("property_type", type_updates)):
        for i in range(0, len(updates), 200):
            sb.table("properties").upsert(updates[i:i + 200]).execute()
        print(f"  wrote {len(updates)} {label} values")
    print("Done.")


# ============================================================
# 3. apn-pr (PAID)
# ============================================================

def pr_lookup(radar_id, purchase):
    resp = requests.get(
        f"{PR_BASE_URL}/{radar_id}",
        headers={"Authorization": f"Bearer {PROPERTYRADAR_API_KEY}", "Accept": "application/json"},
        params={"Fields": "APN,PType", "Purchase": purchase},
        timeout=20,
    )
    return resp


def load_pr_log():
    done = {}
    if os.path.exists(PR_LOG):
        with open(PR_LOG, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    done[rec["radar_id"]] = rec
                except (json.JSONDecodeError, KeyError):
                    pass
    return done


def cmd_apn_pr(args):
    if not PROPERTYRADAR_API_KEY:
        sys.exit("ERROR: PROPERTYRADAR_API_KEY not set in .env")
    sb = get_supabase()

    missing = fetch_all(sb, "radar_id, address, property_type", lambda q: q.is_("apn", "null"))
    already_paid = load_pr_log()

    if args.probe:
        if not missing:
            sys.exit("No rows missing an APN - nothing to probe.")
        rid = missing[0]["radar_id"]
        print(f"Probe (Purchase=0) for {rid}:")
        resp = pr_lookup(rid, purchase=0)
        print(resp.status_code)
        print(json.dumps(resp.json(), indent=2)[:3000] if resp.headers.get("content-type", "").startswith("application/json") else resp.text[:3000])
        return

    banner(args.commit)

    # Rows that were paid for in an earlier run but whose Supabase write
    # failed get re-applied from the log for free.
    replay = [r for r in missing if r["radar_id"] in already_paid and already_paid[r["radar_id"]].get("apn")]
    to_buy = [r for r in missing if r["radar_id"] not in already_paid]
    if args.limit:
        to_buy = to_buy[: args.limit]

    print(f"Rows missing APN:                     {len(missing)}")
    print(f"  already paid (in {PR_LOG}), replay free: {len(replay)}")
    print(f"  will be PURCHASED this run:          {len(to_buy)}")
    if not args.commit:
        for r in to_buy[:20]:
            print(f"    {r['radar_id']}  {r.get('address')}")
        if len(to_buy) > 20:
            print(f"    ... and {len(to_buy) - 20} more")
        print("\nDry run - re-run with --commit to purchase.")
        return

    def apply(radar_id, rec, current_type):
        update = {"radar_id": radar_id, "apn": rec["apn"]}
        if not current_type and rec.get("property_type"):
            update["property_type"] = rec["property_type"]
        sb.table("properties").update(update).eq("radar_id", radar_id).execute()

    for r in replay:
        apply(r["radar_id"], already_paid[r["radar_id"]], r.get("property_type"))

    ok = fail = 0
    for i, r in enumerate(to_buy, 1):
        rid = r["radar_id"]
        try:
            resp = pr_lookup(rid, purchase=1)
        except requests.exceptions.RequestException as e:
            print(f"  [{i}/{len(to_buy)}] {rid} network error: {e}")
            fail += 1
            continue

        results = resp.json().get("results", []) if resp.status_code == 200 else []
        raw = results[0] if results else {}
        rec = {
            "radar_id": rid,
            "status_code": resp.status_code,
            "apn": (raw.get("APN") or "").strip() or None,
            "property_type": PTYPE_TO_ABBREVIATION.get(raw.get("PType"), raw.get("PType")),
            "at": datetime.utcnow().isoformat(),
        }
        # Log FIRST - this line is what prevents a second charge later
        with open(PR_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")

        if rec["apn"]:
            apply(rid, rec, r.get("property_type"))
            ok += 1
            print(f"  [{i}/{len(to_buy)}] {rid} -> {rec['apn']}")
        else:
            fail += 1
            print(f"  [{i}/{len(to_buy)}] {rid} no APN returned (HTTP {resp.status_code})")

    print(f"\nDone. {ok} APNs written, {fail} without an APN. Log: {PR_LOG}")


# ============================================================
# 4. mls
# ============================================================

def cmd_mls(args):
    sb = get_supabase()
    client = RealtyfeedClient()
    if not client.configured:
        sys.exit("ERROR: REALTYFEED_CLIENT_ID / REALTYFEED_CLIENT_SECRET not set in .env")
    banner(args.commit)

    cols = "radar_id, address, apn, prior_transfer_date, latest_transfer_date"
    if args.radar_id:
        rows = fetch_all(sb, cols, lambda q: q.eq("radar_id", args.radar_id))
    elif args.all:
        rows = fetch_all(sb, cols)
    else:
        rows = fetch_all(sb, cols, lambda q: q.is_("mls_checked_at", "null"))
    if args.limit:
        rows = rows[: args.limit]

    print(f"Properties to check: {len(rows)}\n")
    tally = {}
    for i, row in enumerate(rows, 1):
        s = enrich_property(sb, client, row, write=args.commit)
        st = s.get("mls_status")
        tally[st] = tally.get(st, 0) + 1
        print(
            f"  [{i}/{len(rows)}] {row['radar_id']} {row.get('address') or '':<30.30} "
            f"{st:<22} DOM={s.get('days_on_market')} ({s.get('dom_source')}) "
            f"L/S={s.get('list_to_sold_ratio')} listings={s.get('mls_listing_count')}"
        )

    print("\nSummary:", ", ".join(f"{k}: {v}" for k, v in sorted(tally.items(), key=lambda kv: str(kv[0]))))
    if not args.commit:
        print("Dry run - re-run with --commit to write.")


# ============================================================

def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("mls-test", help="Query the MLS for one APN and print the raw result")
    t.add_argument("--apn", required=True)
    t.set_defaults(func=cmd_mls_test)

    c = sub.add_parser("apn-csv", help="FREE: fill APN from PropertyRadar export CSVs")
    c.add_argument("--csv", action="append", required=True)
    c.add_argument("--overwrite", action="store_true", help="Replace APNs that are already set")
    c.add_argument("--commit", action="store_true")
    c.set_defaults(func=cmd_apn_csv)

    r = sub.add_parser("apn-pr", help="PAID: fetch missing APNs from PropertyRadar")
    r.add_argument("--probe", action="store_true", help="One Purchase=0 call to inspect the response")
    r.add_argument("--limit", type=int)
    r.add_argument("--commit", action="store_true")
    r.set_defaults(func=cmd_apn_pr)

    m = sub.add_parser("mls", help="Enrich rows with MLS days-on-market + list-to-sold")
    m.add_argument("--limit", type=int)
    m.add_argument("--radar-id")
    m.add_argument("--all", action="store_true", help="Re-check rows that were already checked")
    m.add_argument("--commit", action="store_true")
    m.set_defaults(func=cmd_mls)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()