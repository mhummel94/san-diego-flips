create policy flip_readonly_select on mls_listings
    for select to flip_readonly using (true);