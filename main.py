"""
PropertyRadar -> Railway -> Supabase pipeline
------------------------------------------------
Receives a webhook from PropertyRadar every time a new property matches a
monitored Dynamic List, fetches that property's ownership-transfer history
(same filtering logic validated in propertyradar_transaction_pull.py), and
upserts the result into a Supabase Postgres table. Also serves a token-
protected CSV export of the whole table.

RETENTION: rows in `properties` older than 18 months (based on
latest_transfer_date, or updated_at for rows with no transfer date) are
automatically deleted by a daily background job. This keeps the analytical
table lean and relevant. The webhook's cost-safeguard does NOT depend on
`properties` for this reason — see processed_radar_ids below.

ENDPOINTS
  POST /webhook/propertyradar   <- PropertyRadar sends its "New Match" payload here
  GET  /export.csv?token=...    <- streams the current table as a CSV
  GET  /health                  <- for Railway's health check

ENVIRONMENT VARIABLES (set these in Railway's dashboard, not in code)
  PROPERTYRADAR_API_KEY      your PropertyRadar API key (same one used locally)
  PROPERTYRADAR_WEBHOOK_SECRET   the Webhook Secret you set in PropertyRadar's
                              Add Integration dialog (Account Settings ->
                              Integrations & API)
  SUPABASE_URL                from Supabase project settings -> API
  SUPABASE_SERVICE_KEY        the SERVICE ROLE key (not the anon key — this
                              service needs write access), from the same page
  EXPORT_TOKEN                 a password you make up, required as
                              ?token=... on /export.csv
  REALTYFEED_CLIENT_ID        MLS (Realtyfeed) client_id - see mls.py
  REALTYFEED_CLIENT_SECRET    MLS (Realtyfeed) client_secret
  REALTYFEED_API_KEY          OPTIONAL, only if your account needs x-api-key

MLS ENRICHMENT (Phase 2):
  After a new property is processed and recorded in the ledger, its APN is
  looked up in the MLS in a background task (so PropertyRadar's webhook
  gets its 200 immediately). An MLS failure can never cause a PropertyRadar
  re-charge: the ledger is written BEFORE the MLS step runs. Logic lives in
  mls.py and is shared with phase1_backfill.py.

NOTE ON THE WEBHOOK SECRET HEADER:
  Confirmed against a real PropertyRadar webhook delivery: they send the
  Webhook Secret as a standard "Authorization: Bearer <secret>" header.
  This is what the code below checks.

NOTE ON processed_radar_ids:
  This is a small, PERMANENT table (never pruned) that exists purely to
  remember "we already paid PropertyRadar for this RadarID at least once."
  It's checked BEFORE the paid transactions call, and is separate from
  `properties` specifically so that pruning old analytical data can never
  cause a RadarID to look "new" again and get double-charged.
"""

import csv
import io
import json
import os
import time
import threading
from datetime import datetime, timedelta

import requests
from fastapi import FastAPI, Request, HTTPException, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from supabase import create_client, Client

from mls import RealtyfeedClient, enrich_property

app = FastAPI()

# ============================================================
# Config (from environment — never hardcode secrets here)
# ============================================================
PROPERTYRADAR_API_KEY = os.environ.get("PROPERTYRADAR_API_KEY", "")
WEBHOOK_SECRET = os.environ.get("PROPERTYRADAR_WEBHOOK_SECRET", "")
EXPORT_TOKEN = os.environ.get("EXPORT_TOKEN", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

BASE_URL = "https://api.propertyradar.com/v1/properties"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if SUPABASE_URL else None
mls_client = RealtyfeedClient()  # reads REALTYFEED_* env vars; no-op if unset


def pr_headers():
    return {"Authorization": f"Bearer {PROPERTYRADAR_API_KEY}", "Accept": "application/json"}


# Only mapping types actually confirmed in our real data so far. Anything
# else passes through as PropertyRadar's raw PType value rather than
# guessing at an abbreviation we've never verified.
PTYPE_TO_ABBREVIATION = {
    "Single Family": "SFR",
    "Condominium": "CND",
}


def to_num_or_none(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fetch_property_specs(radar_id):
    """
    Real, paid call (Purchase=1, charged per record returned) to
    PropertyRadar's single-property lookup endpoint. Fetches fields that
    are NOT reliably present in the webhook's own "New Match" payload:
    property type, pool, lot size, garage size, stories, and APN. APN rides
    along on this same call (PropertyRadar charges per record, not per
    field), so capturing it adds no extra PropertyRadar call.
    """
    url = f"{BASE_URL}/{radar_id}"
    params = {"Fields": "PType,Pool,LotSize,GarageSize,Stories,APN", "Purchase": 1}

    try:
        resp = requests.get(url, headers=pr_headers(), params=params, timeout=20)
    except requests.exceptions.RequestException as e:
        print(f"  [network error fetching specs for {radar_id}] {e}")
        return {}

    if resp.status_code != 200:
        print(f"  [error {resp.status_code} fetching specs for {radar_id}] {resp.text[:200]}")
        return {}

    results = resp.json().get("results", [])
    if not results:
        return {}

    raw = results[0]
    ptype_raw = raw.get("PType")
    pool_raw = raw.get("Pool")

    return {
        "property_type": PTYPE_TO_ABBREVIATION.get(ptype_raw, ptype_raw),
        "pool": "Yes" if pool_raw == 1 else "No" if pool_raw == 0 else None,
        "lot_sqft": to_num_or_none(raw.get("LotSize")),
        "garage": to_num_or_none(raw.get("GarageSize")),
        "stories": to_num_or_none(raw.get("Stories")),
        "apn": (raw.get("APN") or "").strip() or None,
    }


# ============================================================
# Transfer-extraction logic — identical to propertyradar_transaction_pull.py,
# validated against the Foxwood test case. Keep these two files in sync if
# either is updated.
# ============================================================

def is_ownership_transfer(txn):
    doc_type = (txn.get("DocTypeUI") or "")
    purpose = (txn.get("Purpose") or "")
    amount = txn.get("Amount")
    grantor = (txn.get("Grantor") or "").strip().upper()
    grantee = (txn.get("Grantee") or "").strip().upper()

    if doc_type.startswith("- "):
        return False
    if doc_type == "Loan":
        return False
    if "Deed" not in doc_type and doc_type not in ("Certificate of Title", "Exchange", "Conveyance"):
        return False
    if not amount:
        return False
    try:
        if float(amount) <= 0:
            return False
    except (TypeError, ValueError):
        return False
    if grantor and grantor == grantee:
        return False
    if purpose.startswith("NonMarket") or purpose.startswith("Non-Market"):
        return False
    return True


def top_two_transfers(transactions):
    transfers = [t for t in transactions if is_ownership_transfer(t)]
    transfers.sort(key=lambda t: t.get("RecDate") or "", reverse=True)
    return transfers[:2]


def fetch_transactions(radar_id):
    """Live call, Purchase=1 — this spends money each time it runs."""
    url = f"{BASE_URL}/{radar_id}/transactions"
    params = {"Filter": "All", "Purchase": 1}
    resp = requests.get(url, headers=pr_headers(), params=params, timeout=20)
    resp.raise_for_status()
    return resp.json().get("results", [])


# ============================================================
# Core processing
# ============================================================

def process_radar_id(radar_id, payload):
    txns = fetch_transactions(radar_id)
    top2 = top_two_transfers(txns)
    specs = fetch_property_specs(radar_id)

    record = {
        "radar_id": radar_id,
        "address": payload.get("Address"),
        "city": payload.get("City"),
        "state": payload.get("State"),
        "zip": payload.get("ZipFive") or payload.get("Zip") or payload.get("ZIP"),
        "sqft": payload.get("SqFt"),
        "year_built": payload.get("YearBuilt"),
        "beds": payload.get("Beds"),
        "baths": payload.get("Baths"),
        "property_type": specs.get("property_type"),
        "pool": specs.get("pool"),
        "lot_sqft": specs.get("lot_sqft"),
        "garage": specs.get("garage"),
        "stories": specs.get("stories"),
        "apn": specs.get("apn") or (payload.get("APN") or "").strip() or None,
        "updated_at": datetime.utcnow().isoformat(),
    }

    if not top2:
        record["status"] = "no_transfers_found"
        supabase.table("properties").upsert(record).execute()
        return record

    latest = top2[0]
    prior = top2[1] if len(top2) > 1 else None

    record.update({
        "status": "complete" if prior else "partial_one_transfer",
        "latest_transfer_date": latest.get("RecDate"),
        "latest_price": latest.get("Amount"),
        "latest_seller": latest.get("Grantor"),
        "latest_buyer": latest.get("Grantee"),
    })

    if prior:
        record.update({
            "prior_transfer_date": prior.get("RecDate"),
            "prior_price": prior.get("Amount"),
            "prior_seller": prior.get("Grantor"),
            "prior_buyer": prior.get("Grantee"),
        })
        try:
            latest_amt = float(latest.get("Amount"))
            prior_amt = float(prior.get("Amount"))
            record["spread_amount"] = latest_amt - prior_amt
            if prior_amt:
                record["spread_pct"] = round((latest_amt - prior_amt) / prior_amt * 100, 2)
        except (TypeError, ValueError):
            pass
        try:
            d1 = datetime.strptime(latest.get("RecDate")[:10], "%Y-%m-%d")
            d2 = datetime.strptime(prior.get("RecDate")[:10], "%Y-%m-%d")
            record["days_held"] = (d1 - d2).days
        except (TypeError, ValueError, AttributeError):
            pass

    supabase.table("properties").upsert(record).execute()
    return record


# ============================================================
# Routes
# ============================================================

@app.post("/webhook/propertyradar")
async def propertyradar_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Confirmed against a real PropertyRadar webhook: they send the secret
    # as a standard "Authorization: Bearer <secret>" header.
    if WEBHOOK_SECRET:
        auth_header = request.headers.get("authorization", "")
        expected = f"Bearer {WEBHOOK_SECRET}"
        if auth_header != expected:
            raise HTTPException(status_code=401, detail="Webhook secret did not match")

    radar_id = payload.get("RadarID")
    if not radar_id:
        raise HTTPException(status_code=400, detail="No RadarID in payload")

    # Safeguard: check the PERMANENT ledger (processed_radar_ids), not
    # `properties`. This is deliberate — `properties` gets pruned after 18
    # months, but the memory of "we already paid for this RadarID" must
    # never be lost, or a re-sent webhook for an old (now-pruned) property
    # would look "new" and trigger a duplicate charge.
    existing_ledger = supabase.table("processed_radar_ids").select("radar_id").eq("radar_id", radar_id).execute()
    if existing_ledger.data:
        print(f"Skipping {radar_id} — already processed per ledger, no charge made.")
        return {"ok": True, "radar_id": radar_id, "status": "skipped_already_processed"}

    # Write a pending row immediately so the event is never lost even if
    # the transaction-history pull below fails or times out.
    supabase.table("properties").upsert({
        "radar_id": radar_id,
        "address": payload.get("Address"),
        "status": "pending",
    }).execute()

    try:
        record = process_radar_id(radar_id, payload)
    except Exception as e:
        supabase.table("properties").upsert({"radar_id": radar_id, "status": "error"}).execute()
        # Deliberately NOT written to processed_radar_ids — an error means
        # we didn't get a real result, so this RadarID should be retried
        # on a future webhook delivery rather than permanently skipped.
        raise HTTPException(status_code=500, detail=str(e))

    # Record in the permanent ledger — this RadarID has now been paid for,
    # regardless of what the actual result was (complete, partial, or even
    # no transfers found — PropertyRadar still charges per record returned
    # on the query itself).
    supabase.table("processed_radar_ids").upsert({
        "radar_id": radar_id,
        "last_status": record.get("status"),
    }).execute()

    # Phase 2: MLS enrichment, AFTER the ledger write so an MLS problem can
    # never make this RadarID look unpaid. Runs in the background so the
    # webhook responds immediately.
    if mls_client.configured:
        background_tasks.add_task(run_mls_enrichment, record)

    return {"ok": True, "radar_id": radar_id, "status": record.get("status")}


def run_mls_enrichment(record):
    try:
        summary = enrich_property(supabase, mls_client, record)
        print(f"[mls] {record['radar_id']}: {summary.get('mls_status')} "
              f"DOM={summary.get('days_on_market')} L/S={summary.get('list_to_sold_ratio')}")
    except Exception as e:
        # Never crash the service over enrichment; the row just keeps
        # mls_status null and can be picked up by `phase1_backfill.py mls`.
        print(f"[mls] ERROR enriching {record.get('radar_id')}: {e}")


@app.get("/export.csv")
def export_csv(token: str = Query(...)):
    if not EXPORT_TOKEN or token != EXPORT_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    # Supabase returns at most 1000 rows per request, so page through
    # everything (the table is already past 1000 rows).
    data, start, page = [], 0, 1000
    while True:
        batch = supabase.table("properties").select("*").order("radar_id").range(start, start + page - 1).execute().data or []
        data.extend(batch)
        if len(batch) < page:
            break
        start += page

    output = io.StringIO()
    if data:
        writer = csv.DictWriter(output, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=propertyradar_export.csv"},
    )


@app.get("/health")
def health():
    return {"status": "ok"}


# ============================================================
# Retention: prune properties older than 18 months
# ============================================================

RETENTION_MONTHS = 18


def prune_old_properties():
    """
    Deletes rows from `properties` where the data is stale:
      - latest_transfer_date is more than 18 months ago, OR
      - latest_transfer_date is null (no_transfers_found/error/pending rows)
        AND the row itself hasn't been touched in 18 months
    Never touches processed_radar_ids — that ledger is permanent by design.
    """
    cutoff = (datetime.utcnow() - timedelta(days=RETENTION_MONTHS * 30)).date().isoformat()

    try:
        result = (
            supabase.table("properties")
            .delete()
            .lt("latest_transfer_date", cutoff)
            .execute()
        )
        deleted_with_date = len(result.data or [])

        result2 = (
            supabase.table("properties")
            .delete()
            .is_("latest_transfer_date", "null")
            .lt("updated_at", cutoff)
            .execute()
        )
        deleted_without_date = len(result2.data or [])

        total = deleted_with_date + deleted_without_date
        if total:
            print(f"[prune] Deleted {total} stale properties (cutoff: {cutoff})")
    except Exception as e:
        print(f"[prune] ERROR during pruning: {e}")


def _pruning_loop():
    # Run once shortly after startup, then once every 24 hours. This is a
    # plain in-process scheduler (no external cron dependency, no
    # assumptions about Supabase plan tier) — reliable as long as the
    # Railway service itself is running, which it always is.
    time.sleep(60)  # brief delay so startup isn't slowed down
    while True:
        prune_old_properties()
        time.sleep(24 * 60 * 60)


if supabase:
    threading.Thread(target=_pruning_loop, daemon=True).start()