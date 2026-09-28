"""Run with:  python -m pytest tests/  (or: python tests/test_mls_chain.py)"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mls import build_chain, apn_to_digits

def L(key, lid, on, off=None, contract=None, close=None, olp=None, lp=None, cp=None,
      cdom=None, dom=None, status="Closed", tt="For Sale", entry=None):
    return {"OriginalEntryTimestamp": entry, "ListingKey": key, "ListingId": lid, "OnMarketDate": on, "OffMarketDate": off,
            "PurchaseContractDate": contract, "CloseDate": close, "OriginalListPrice": olp,
            "ListPrice": lp, "ClosePrice": cp, "CumulativeDaysOnMarket": cdom,
            "DaysOnMarket": dom, "StandardStatus": status, "RFTransactionType": tt}

PRIOR, LATEST = "2025-08-06", "2026-01-20"

def test_apn():
    assert apn_to_digits("157-791-64-00") == "1577916400"

def test_single_listing_clean_cdom():
    s, _ = build_chain([L("1","A","2025-11-01",contract="2025-11-21",close="2026-01-15",
                          olp=1_050_000,cp=1_000_000,cdom=20)], PRIOR, LATEST)
    assert s["mls_status"] == "matched" and s["days_on_market"] == 75   # Nov 1 -> Jan 15 close
    assert s["list_to_sold_ratio"] == round(1_000_000/1_050_000, 4)

def test_relist_merged_and_prefix_fixer_excluded():
    raws = [
        # pre-flip fixer listing (flipper bought it) - must be excluded
        L("0","F","2025-06-01",contract="2025-06-20",close="2025-08-05",olp=800_000,cp=775_000),
        # flipper's first try, expired
        L("1","A","2025-10-01",off="2025-11-10",olp=1_150_000,status="Expired"),
        # relisted 12 days later at a lower price, sold. CDOM reset by MLS -> dirty
        L("2","B","2025-11-22",contract="2025-12-12",close="2026-01-18",olp=1_050_000,cp=1_000_000,cdom=20),
    ]
    s, ls = build_chain(raws, PRIOR, LATEST)
    assert s["mls_listing_count"] == 2
    assert s["mls_highest_list_price"] == 1_150_000          # highest OLP across chain
    assert s["days_on_market"] == 40 + 57                    # 40 days listing A + Nov 22->Jan 18 close; 12-day gap excluded
    assert s["list_to_sold_ratio"] == round(1_000_000/1_150_000, 4)
    assert [l["in_chain"] for l in ls] == [False, True, True]

def test_gap_too_big_not_merged():
    raws = [L("1","A","2025-08-20",off="2025-09-10",olp=1_300_000,status="Canceled"),
            L("2","B","2025-11-01",contract="2025-11-15",close="2026-01-10",olp=1_050_000,cp=1_000_000,cdom=14)]
    s, _ = build_chain(raws, PRIOR, LATEST)
    assert s["mls_listing_count"] == 1 and s["mls_highest_list_price"] == 1_050_000

def test_duplicate_feed_record_not_double_counted():
    raws = [L("1","A","2025-11-01",contract="2025-11-21",close="2026-01-15",olp=1_050_000,cp=1_000_000),
            L("1b","A","2025-11-01",contract="2025-11-21",close="2026-01-15",olp=1_050_000,cp=1_000_000)]
    s, _ = build_chain(raws, PRIOR, LATEST)
    assert s["mls_listing_count"] == 1 and s["days_on_market"] == 75

def test_lease_ignored_and_not_found():
    s, _ = build_chain([L("9","R","2025-12-01",close="2025-12-15",cp=4000,tt="For Rent")], PRIOR, LATEST)
    assert s["mls_status"] == "no_listing_in_window"
    assert build_chain([], PRIOR, LATEST)[0]["mls_status"] == "not_found"

def test_cdom_ignored():
    # CDOM is stored for reference only; DOM always runs listed -> closed
    s, _ = build_chain([L("1","A","2025-11-01",contract="2025-11-21",close="2026-01-15",
                          olp=1e6,cp=1e6,cdom=200)], PRIOR, LATEST)
    assert s["days_on_market"] == 75 and s["mls_cdom"] == 200

def test_bad_on_market_date_uses_mls_entry_date():
    # Real case (4920 Elsa Rd): OnMarketDate a year before the listing was even entered
    s, _ = build_chain([L("1","A","2025-01-11",contract="2026-01-20",close="2026-02-05",
                          olp=1e6,cp=1e6,cdom=1,dom=366,entry="2026-01-12T00:00:01Z")],
                       "2025-10-01", "2026-02-05")
    assert s["mls_first_on_market"] == "2026-01-12"
    assert s["days_on_market"] == 24                         # Jan 12 entry -> Feb 5 close

def test_coming_soon_keeps_on_market_date():
    # Entered as Coming Soon Nov 1, went active Nov 10 -> DOM counts from Nov 10
    s, _ = build_chain([L("1","A","2025-11-10",contract="2025-11-30",close="2026-01-15",
                          olp=1e6,cp=1e6,entry="2025-11-01T00:00:01Z")], PRIOR, LATEST)
    assert s["mls_first_on_market"] == "2025-11-10" and s["days_on_market"] == 66

def test_off_market_sale_recorded_after_contract():
    # Real case (29456 Pacific Crest Way): contract Mar 18, entered in MLS Apr 11 on closing day
    s, _ = build_chain([L("1","A","2025-04-11",contract="2025-03-18",close="2025-04-11",
                          olp=1e6,cp=1e6,entry="2025-04-11T00:00:01Z")], "2024-12-01", "2025-04-11")
    assert s["mls_status"] == "off_market_sale"
    assert s.get("days_on_market") is None and s.get("list_to_sold_ratio") is None

def test_same_day_reentry_after_real_listing_still_matched():
    # Elsa Rd pattern: real listing Dec 9-Jan 11, re-entered and under contract Jan 12
    raws = [L("1","A","2025-12-09",off="2026-01-11",olp=999_999,status="Withdrawn"),
            L("2","B","2026-01-12",contract="2026-01-12",close="2026-02-05",olp=1_100_000,cp=1_060_000)]
    s, _ = build_chain(raws, "2025-10-20", "2026-02-05")
    assert s["mls_status"] == "matched" and s["days_on_market"] == 33 + 24

if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); print("PASS", name)