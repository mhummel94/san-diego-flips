"""
MLS enrichment via Realtyfeed (MLS Router API)
------------------------------------------------
Given a property's APN, pulls every MLS listing record for that parcel,
decides which records belong to the flipper's resale (merging relists),
and computes:

  days_on_market      - CumulativeDaysOnMarket if it looks clean, otherwise
                        our own OnMarketDate -> PurchaseContractDate calc
  list_to_sold_ratio  - ClosePrice / highest OriginalListPrice in the chain

Used by BOTH main.py (live webhook, Phase 2) and phase1_backfill.py
(one-off backfill, Phase 1), so the logic can never drift between them.

ENVIRONMENT VARIABLES
  REALTYFEED_CLIENT_ID      client_id for the token endpoint
  REALTYFEED_CLIENT_SECRET  client_secret for the token endpoint
  REALTYFEED_API_KEY        OPTIONAL - only set this if your account also
                            requires an x-api-key header on data requests

HOW THE LISTING CHAIN IS BUILT (all decisions use MLS dates)
  1. Keep only For Sale records (drops lease listings on the same parcel).
  2. Anchor = the Closed listing whose CloseDate is nearest the recorded
     resale date (latest_transfer_date), within CLOSE_MATCH_DAYS.
  3. Walk backwards from the anchor. An earlier listing is merged in if
     it came on market on/after the flipper's purchase (prior_transfer_date)
     and the gap between its off-market date and the next listing's
     on-market date is <= RELIST_GAP_DAYS. Stop at the first gap that's too
     big, or at an earlier Closed listing (that's a different sale).
  4. Duplicate records for the same listing (same ListingId from two
     originating systems) overlap in time; computed DOM uses the UNION of
     on-market intervals, so overlaps are never double-counted.
"""

import os
import time
from datetime import date, datetime, timedelta

import requests

TOKEN_URL = "https://api.realtyfeed.com/v1/auth/token"
PROPERTY_URL = "https://api.realtyfeed.com/reso/odata/Property"

RELIST_GAP_DAYS = 30      # "within a month" -> same marketing effort
CLOSE_MATCH_DAYS = 45     # MLS CloseDate vs county recording date tolerance
CDOM_TOLERANCE_DAYS = 7   # how far CDOM may drift from our own calc and still count as "clean"

SELECT_FIELDS = [
    # The three the metrics are built from
    "CumulativeDaysOnMarket", "OriginalListPrice", "ClosePrice",
    # Needed to decide which records belong together, and for the fallback DOM calc
    "ListingKey", "ListingId", "ParcelNumber", "StandardStatus", "MlsStatus",
    "OnMarketDate", "OriginalEntryTimestamp", "OffMarketDate",
    "PurchaseContractDate", "CloseDate", "ListPrice", "DaysOnMarket",
    "RFTransactionType", "PropertyType", "OriginatingSystemName",
]


# ============================================================
# Realtyfeed client
# ============================================================

class RealtyfeedClient:
    def __init__(self, client_id=None, client_secret=None, api_key=None):
        self.client_id = client_id or os.environ.get("REALTYFEED_CLIENT_ID", "")
        self.client_secret = client_secret or os.environ.get("REALTYFEED_CLIENT_SECRET", "")
        self.api_key = api_key if api_key is not None else os.environ.get("REALTYFEED_API_KEY", "")
        self._token = None
        self._token_expires_at = 0.0

    @property
    def configured(self):
        return bool(self.client_id and self.client_secret)

    def _get_token(self):
        # Refresh 60s early so a token never expires mid-request
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token

        resp = requests.post(
            TOKEN_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data={"client_id": self.client_id, "client_secret": self.client_secret},
            timeout=20,
        )
        resp.raise_for_status()
        body = resp.json()
        token = body.get("access_token") or (body.get("data") or {}).get("access_token")
        if not token:
            raise RuntimeError(f"Realtyfeed token response had no access_token: {str(body)[:300]}")
        self._token = token
        self._token_expires_at = time.time() + float(body.get("expires_in") or 3600)
        return token

    def _headers(self):
        h = {"Authorization": f"Bearer {self._get_token()}", "Accept": "application/json"}
        if self.api_key:
            h["x-api-key"] = self.api_key
        return h

    def query_parcel(self, parcel_number, select=None):
        """Returns the raw list of Property records for one ParcelNumber."""
        safe = str(parcel_number).replace("'", "''")  # OData string escaping
        params = {
            "$filter": f"ParcelNumber eq '{safe}'",
            "$select": ",".join(select or SELECT_FIELDS),
            "$top": 50,
        }

        for attempt in range(4):
            resp = requests.get(PROPERTY_URL, headers=self._headers(), params=params, timeout=30)
            if resp.status_code == 401 and attempt == 0:
                self._token = None  # token revoked/expired early - get a fresh one once
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep(2 ** attempt)  # 1s, 2s, 4s backoff
                continue
            resp.raise_for_status()
            body = resp.json()
            # Direct API returns {"value": [...]}; some wrappers nest it under "data"
            if isinstance(body, dict) and "value" not in body and isinstance(body.get("data"), dict):
                body = body["data"]
            return body.get("value", []) if isinstance(body, dict) else []

        resp.raise_for_status()
        return []

    def listings_for_apn(self, apn):
        """Tries the digits-only form first (how the MLS stores it), then the
        dashed PropertyRadar form as a fallback in case some records differ."""
        digits = apn_to_digits(apn)
        if not digits:
            return []
        results = self.query_parcel(digits)
        if not results and apn and apn.strip() != digits:
            results = self.query_parcel(apn.strip())
        return results


# ============================================================
# Pure helpers (no network) - these are what the tests exercise
# ============================================================

def apn_to_digits(apn):
    if not apn:
        return None
    d = "".join(ch for ch in str(apn) if ch.isdigit())
    return d or None


def parse_date(v):
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def to_int(v):
    try:
        return int(float(v)) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def to_num(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def effective_on_market(raw):
    """A listing can't be on market before it was entered in the MLS. Some
    records carry a bad OnMarketDate (e.g. a year too early), so use the
    LATER of OnMarketDate and the MLS entry date. Coming Soon listings are
    entered before they go active, so OnMarketDate still wins for those."""
    on = parse_date(raw.get("OnMarketDate"))
    entered = parse_date(raw.get("OriginalEntryTimestamp"))
    if on and entered:
        return max(on, entered)
    return on or entered

def normalize_listing(raw):
    return {
        "listing_key": str(raw.get("ListingKey") or ""),
        "listing_id": raw.get("ListingId"),
        "parcel_number": raw.get("ParcelNumber"),
        "standard_status": raw.get("StandardStatus"),
                "on_market_date": effective_on_market(raw),
        "off_market_date": parse_date(raw.get("OffMarketDate")),
        "purchase_contract_date": parse_date(raw.get("PurchaseContractDate")),
        "close_date": parse_date(raw.get("CloseDate")),
        "original_list_price": to_num(raw.get("OriginalListPrice")),
        "list_price": to_num(raw.get("ListPrice")),
        "close_price": to_num(raw.get("ClosePrice")),
        "cumulative_days_on_market": to_int(raw.get("CumulativeDaysOnMarket")),
        "days_on_market": to_int(raw.get("DaysOnMarket")),
        "_is_sale": _is_sale(raw),
        "_raw": raw,
    }


def _is_sale(raw):
    tt = (raw.get("RFTransactionType") or "").lower()
    pt = (raw.get("PropertyType") or "").lower()
    if "lease" in pt or "rent" in tt or "lease" in tt:
        return False
    return True


def _is_closed(l):
    return (l["standard_status"] or "").lower() == "closed" or l["close_date"] is not None


def _listing_end(l):
    """When an earlier (non-selling) listing stopped being on market."""
    if l["off_market_date"]:
        return l["off_market_date"]
    if l["on_market_date"] and l["days_on_market"] is not None:
        return l["on_market_date"] + timedelta(days=l["days_on_market"])
    return None


def _union_days(intervals):
    """Total days covered by possibly-overlapping [start, end] intervals."""
    ivs = sorted((s, e) for s, e in intervals if s and e and e >= s)
    total, cur_s, cur_e = 0, None, None
    for s, e in ivs:
        if cur_s is None:
            cur_s, cur_e = s, e
        elif s <= cur_e:
            cur_e = max(cur_e, e)
        else:
            total += (cur_e - cur_s).days
            cur_s, cur_e = s, e
    if cur_s is not None:
        total += (cur_e - cur_s).days
    return total


def build_chain(raw_listings, prior_transfer_date, latest_transfer_date):
    """
    Returns (summary_dict, normalized_listings). Pure function - no I/O.
    summary_dict["mls_status"] is one of:
      matched | not_found | no_listing_in_window
    (enrich_property can also set: no_apn | no_transfer_dates | error)
    """
    prior = parse_date(prior_transfer_date)
    latest = parse_date(latest_transfer_date)

    listings = [normalize_listing(r) for r in raw_listings]
    for l in listings:
        l["in_chain"] = False

    if not listings:
        return {"mls_status": "not_found"}, listings

    sale = [l for l in listings if l["_is_sale"]]

    # --- 1. Anchor: the closed listing matching the recorded resale ---------
    closed = [l for l in sale if _is_closed(l) and l["close_date"]]
    if latest:
        closed = [l for l in closed if abs((l["close_date"] - latest).days) <= CLOSE_MATCH_DAYS]
        closed.sort(key=lambda l: abs((l["close_date"] - latest).days))
    else:
        closed.sort(key=lambda l: l["close_date"], reverse=True)

    if not closed:
        return {"mls_status": "no_listing_in_window"}, listings

    anchor = closed[0]
    chain = [anchor]

    # Duplicate feed records for the anchor itself (same ListingId) belong too
    for l in sale:
        if l is not anchor and anchor["listing_id"] and l["listing_id"] == anchor["listing_id"]:
            chain.append(l)

    # --- 2. Walk backwards merging relists ----------------------------------
    earlier = [
        l for l in sale
        if all(l is not c for c in chain)
        and l["on_market_date"]
        and anchor["on_market_date"]
        and l["on_market_date"] < anchor["on_market_date"]
        and (prior is None or l["on_market_date"] >= prior)
    ]
    earlier.sort(key=lambda l: l["on_market_date"], reverse=True)

    current_start = min(l["on_market_date"] for l in chain if l["on_market_date"]) if anchor["on_market_date"] else None
    for l in earlier:
        if current_start is None:
            break
        if _is_closed(l):
            break  # an earlier closed sale is a different transaction
        end = _listing_end(l)
        if end is None:
            break  # can't judge the gap from MLS data -> don't guess
        gap = (current_start - end).days
        if gap <= RELIST_GAP_DAYS:  # negative gap = overlapping records, also same effort
            chain.append(l)
            current_start = min(current_start, l["on_market_date"])
        else:
            break

    for l in chain:
        l["in_chain"] = True

        # --- 3. Days on market: listed -> CLOSED ------------------------------
    # One number covering the full time the property was tied up in the
    # sale: days actively listed across merged relists (off-market gaps
    # between relists excluded) PLUS escrow through the close date. Any
    # escrow that fell through mid-listing is included too.
    # CumulativeDaysOnMarket is still stored (mls_cdom) for reference, but
    # isn't used - the MLS stops counting at contract, so it can't include
    # escrow.
    first_on = min((l["on_market_date"] for l in chain if l["on_market_date"]), default=None)
    contract = anchor["purchase_contract_date"]
    close = anchor["close_date"]

        # Off-market sale: the only MLS record was entered on/after the day the
    # buyer went under contract, i.e. the deal happened off-market and was
    # entered afterwards just to record it. No real MLS exposure, so DOM and
    # list-to-sold would be meaningless - leave them blank.
    if first_on and contract and first_on >= contract:
        for l in chain:
            l["in_chain"] = True
        return {
            "mls_status": "off_market_sale",
            "mls_close_price": anchor["close_price"],
            "mls_listing_count": len({l["listing_id"] or l["listing_key"] for l in chain}),
            "mls_listing_keys": sorted({l["listing_key"] for l in chain if l["listing_key"]}),
            "mls_first_on_market": first_on.isoformat(),
            "mls_contract_date": contract.isoformat(),
            "mls_close_date": close.isoformat() if close else None,
        }, listings
    
        days_on_market = None
    if first_on and close and close >= first_on:
        intervals = []
        for l in chain:
            if l is anchor or (anchor["listing_id"] and l["listing_id"] == anchor["listing_id"]):
                intervals.append((l["on_market_date"], close))
            else:
                intervals.append((l["on_market_date"], _listing_end(l)))
        days_on_market = _union_days(intervals)

    cdom = anchor["cumulative_days_on_market"]
    dom_source = "listed_to_close" if days_on_market is not None else None
    dom_computed = days_on_market

    # --- 4. List-to-sold ----------------------------------------------------
    list_prices = [
        l["original_list_price"] if l["original_list_price"] is not None else l["list_price"]
        for l in chain
    ]
    list_prices = [p for p in list_prices if p]
    highest = max(list_prices) if list_prices else None
    close_price = anchor["close_price"]
    ratio = round(close_price / highest, 4) if (close_price and highest) else None

    unique_keys = sorted({l["listing_key"] for l in chain if l["listing_key"]})

    summary = {
        "mls_status": "matched",
        "days_on_market": days_on_market,
        "dom_source": dom_source,
        "mls_cdom": cdom,
        "dom_computed": dom_computed,
        "mls_highest_list_price": highest,
        "mls_close_price": close_price,
        "list_to_sold_ratio": ratio,
        "mls_listing_count": len({l["listing_id"] or l["listing_key"] for l in chain}),
        "mls_listing_keys": unique_keys,
        "mls_first_on_market": first_on.isoformat() if first_on else None,
        "mls_contract_date": contract.isoformat() if contract else None,
        "mls_close_date": anchor["close_date"].isoformat() if anchor["close_date"] else None,
    }
    return summary, listings


# ============================================================
# Supabase write-back
# ============================================================

EMPTY_SUMMARY = {
    "days_on_market": None, "dom_source": None, "mls_cdom": None, "dom_computed": None,
    "mls_highest_list_price": None, "mls_close_price": None, "list_to_sold_ratio": None,
    "mls_listing_count": None, "mls_listing_keys": None, "mls_first_on_market": None,
    "mls_contract_date": None, "mls_close_date": None,
}


def _listing_row(radar_id, l):
    def iso(d):
        return d.isoformat() if d else None
    return {
        "radar_id": radar_id,
        "listing_key": l["listing_key"] or f"noKey-{l['listing_id']}",
        "listing_id": l["listing_id"],
        "parcel_number": l["parcel_number"],
        "standard_status": l["standard_status"],
        "on_market_date": iso(l["on_market_date"]),
        "off_market_date": iso(l["off_market_date"]),
        "purchase_contract_date": iso(l["purchase_contract_date"]),
        "close_date": iso(l["close_date"]),
        "original_list_price": l["original_list_price"],
        "list_price": l["list_price"],
        "close_price": l["close_price"],
        "cumulative_days_on_market": l["cumulative_days_on_market"],
        "days_on_market": l["days_on_market"],
        "in_chain": l["in_chain"],
        "raw": l["_raw"],
    }


def enrich_property(supabase, client, row, write=True):
    """
    row needs: radar_id, apn, prior_transfer_date, latest_transfer_date.
    Returns the summary dict. If write=False, nothing touches Supabase.
    """
    radar_id = row["radar_id"]
    now = datetime.utcnow().isoformat()

    if not row.get("apn"):
        summary = {**EMPTY_SUMMARY, "mls_status": "no_apn"}
        listings = []
    elif not row.get("latest_transfer_date"):
        # Without the recorded resale date we can't tell which MLS sale is
        # the flip, so don't guess.
        summary = {**EMPTY_SUMMARY, "mls_status": "no_transfer_dates"}
        listings = []
    else:
        try:
            raw = client.listings_for_apn(row["apn"])
            summary, listings = build_chain(raw, row.get("prior_transfer_date"), row.get("latest_transfer_date"))
            summary = {**EMPTY_SUMMARY, **summary}
        except Exception as e:
            print(f"  [mls error for {radar_id}] {e}")
            summary = {"mls_status": "error"}
            listings = []

    if write:
        update = {**summary, "mls_checked_at": now}
        supabase.table("properties").update(update).eq("radar_id", radar_id).execute()
        if listings:
            # Replace this property's audit rows so re-runs never leave stale ones
            supabase.table("mls_listings").delete().eq("radar_id", radar_id).execute()
            seen, rows = set(), []
            for l in listings:
                r = _listing_row(radar_id, l)
                if r["listing_key"] in seen:
                    continue
                seen.add(r["listing_key"])
                rows.append(r)
            supabase.table("mls_listings").upsert(rows).execute()

    return summary