-- ============================================================
-- Migration 001: APN + MLS enrichment
-- Run once in the Supabase SQL Editor (flip-tracking project).
-- Safe to re-run: every statement is idempotent.
-- ============================================================

-- ------------------------------------------------------------
-- 1. APN on the main table
-- ------------------------------------------------------------
-- `apn` keeps PropertyRadar's display format (e.g. 157-791-64-00).
-- `apn_digits` is derived automatically (1577916400) and is what we match
-- against the MLS `ParcelNumber` field, which is stored without dashes.
alter table properties add column if not exists apn text;
alter table properties add column if not exists apn_digits text
    generated always as (nullif(regexp_replace(coalesce(apn, ''), '\D', '', 'g'), '')) stored;

create index if not exists idx_properties_apn_digits on properties (apn_digits);

-- ------------------------------------------------------------
-- 2. MLS summary columns on the main table
-- ------------------------------------------------------------
-- These are the "answer" columns. Everything needed to audit how they
-- were derived lives in `mls_listings` below.
alter table properties add column if not exists days_on_market integer;
alter table properties add column if not exists dom_source text;             -- 'cumulative' | 'computed' | null
alter table properties add column if not exists mls_cdom integer;            -- raw CumulativeDaysOnMarket from the selling listing
alter table properties add column if not exists dom_computed integer;        -- our own OnMarketDate -> PurchaseContractDate calc
alter table properties add column if not exists mls_highest_list_price numeric;  -- max OriginalListPrice across the merged listing chain
alter table properties add column if not exists mls_close_price numeric;
alter table properties add column if not exists list_to_sold_ratio numeric;  -- close / highest list, e.g. 0.9812
alter table properties add column if not exists mls_listing_count integer;   -- how many MLS records were merged into the chain
alter table properties add column if not exists mls_listing_keys text[];
alter table properties add column if not exists mls_first_on_market date;
alter table properties add column if not exists mls_contract_date date;
alter table properties add column if not exists mls_close_date date;
alter table properties add column if not exists mls_status text;             -- matched | no_apn | no_transfer_dates | not_found | no_listing_in_window | error
alter table properties add column if not exists mls_checked_at timestamptz;

create index if not exists idx_properties_mls_status on properties (mls_status);

-- ------------------------------------------------------------
-- 3. Raw MLS listing records (audit trail)
-- ------------------------------------------------------------
-- One row per MLS listing record returned for a property's APN, including
-- ones that were NOT merged into the chain (in_chain = false), so every
-- decision can be checked later. Cascades on delete so the 18-month
-- retention job in main.py cleans these up automatically.
create table if not exists mls_listings (
    listing_key text not null,
    radar_id text not null references properties(radar_id) on delete cascade,
    listing_id text,
    parcel_number text,
    standard_status text,
    on_market_date date,
    off_market_date date,
    purchase_contract_date date,
    close_date date,
    original_list_price numeric,
    list_price numeric,
    close_price numeric,
    cumulative_days_on_market integer,
    days_on_market integer,
    in_chain boolean default false,
    raw jsonb,
    fetched_at timestamptz default now(),
    primary key (radar_id, listing_key)
);

create index if not exists idx_mls_listings_parcel on mls_listings (parcel_number);

-- ------------------------------------------------------------
-- 4. Let the MCP server's read-only role see the new table
-- ------------------------------------------------------------
-- New columns on `properties` are covered by its existing table-level
-- SELECT grant. The new table needs its own grant.
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'flip_readonly') then
        execute 'grant select on mls_listings to flip_readonly';
    end if;
end $$;