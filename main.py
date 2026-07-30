"""
PropertyRadar -> Railway -> Supabase pipeline
------------------------------------------------
Receives a webhook from PropertyRadar every time a new property matches a
monitored Dynamic List, fetches that property's ownership-transfer history
(same filtering logic validated in propertyradar_transaction_pull.py), and
upserts the result into a Supabase Postgres table. Also serves a token-
protected CSV export of the whole table.

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

NOTE ON THE WEBHOOK SECRET HEADER:
  PropertyRadar's docs confirm you can set a Webhook Secret when adding the
  integration, but don't specify the exact header name they send it back in.
  The first real test webhook from PropertyRadar will show us this — until
  then, this code checks a couple of likely header names and logs the full
  header set on any request so we can confirm and lock it down. Check the
  Railway logs after PropertyRadar's first test send.
"""

import csv
import io
import json
import os
from datetime import datetime

import requests
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
from supabase import create_client, Client

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


def pr_headers():
    return {"Authorization": f"Bearer {PROPERTYRADAR_API_KEY}", "Accept": "application/json"}


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
async def propertyradar_webhook(request: Request):
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

    # Safeguard: if this property already has a terminal (paid-for) result,
    # skip the expensive transactions call entirely. This protects against
    # duplicate charges from a re-sent webhook, a retried PropertyRadar
    # delivery, or manually using "Export to Integration" (which re-sends
    # your WHOLE list, not just new items).
    TERMINAL_STATUSES = {"complete", "partial_one_transfer", "no_transfers_found"}
    existing = supabase.table("properties").select("radar_id, status").eq("radar_id", radar_id).execute()
    if existing.data and existing.data[0].get("status") in TERMINAL_STATUSES:
        print(f"Skipping {radar_id} — already processed (status: {existing.data[0].get('status')}), no charge made.")
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
        raise HTTPException(status_code=500, detail=str(e))

    return {"ok": True, "radar_id": radar_id, "status": record.get("status")}


@app.get("/export.csv")
def export_csv(token: str = Query(...)):
    if not EXPORT_TOKEN or token != EXPORT_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    data = supabase.table("properties").select("*").execute().data

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