-- Run this once in Supabase's SQL Editor to create the table.

create table if not exists properties (
    radar_id text primary key,
    address text,
    city text,
    state text,
    zip text,
    property_type text,
    sqft numeric,
    lot_sqft numeric,
    year_built numeric,
    beds numeric,
    baths numeric,
    stories numeric,
    garage numeric,
    pool text,

    latest_transfer_date date,
    latest_price numeric,
    latest_seller text,
    latest_buyer text,

    prior_transfer_date date,
    prior_price numeric,
    prior_seller text,
    prior_buyer text,

    spread_amount numeric,
    spread_pct numeric,
    days_held integer,

    status text default 'pending',   -- pending | complete | partial_one_transfer | no_transfers_found | error

    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

-- Speeds up the CSV export query and any future dashboard filtering
create index if not exists idx_properties_status on properties (status);
create index if not exists idx_properties_updated_at on properties (updated_at);
