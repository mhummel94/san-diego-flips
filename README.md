# PropertyRadar -> Railway -> Supabase Pipeline

Turns a PropertyRadar Dynamic List into a live, growing database of
property transaction spreads — automatically, whenever a new property
matches your list criteria.

## What this does

1. PropertyRadar's Dynamic List gets a new match -> sends a webhook
2. This Railway service receives it, looks up that property's ownership
   transfer history, extracts the 2 most recent genuine sales
3. Writes the result into your Supabase database
4. Anyone with the export link can pull a fresh CSV anytime

## One-time setup

### 1. Supabase
1. Create a project at supabase.com (free tier is fine to start)
2. In the SQL Editor, run `schema.sql` from this folder
3. Go to Project Settings -> API. Copy:
   - **Project URL** -> this is `SUPABASE_URL`
   - **service_role key** (NOT the anon/public key — this needs write
     access) -> this is `SUPABASE_SERVICE_KEY`

### 2. Railway
1. Create a new project, deploy from this folder (or connect your GitHub
   repo if you push this there first)
2. Railway auto-detects Python; it'll run `uvicorn main:app` — if it
   doesn't pick a start command automatically, set it explicitly in
   Railway's settings: `uvicorn main:app --host 0.0.0.0 --port $PORT`
3. In Railway's Variables tab, set:
   - `PROPERTYRADAR_API_KEY` — same key from your local config.json
   - `PROPERTYRADAR_WEBHOOK_SECRET` — make up a long random string,
     you'll enter this same value in PropertyRadar in step 3
   - `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` — from step 1
   - `EXPORT_TOKEN` — make up another random string; this is the
     password anyone needs to download the CSV export
4. Deploy. Railway gives you a public URL like
   `https://your-app.up.railway.app`

### 3. PropertyRadar
1. Make sure your Dynamic List has **Monitoring enabled**
2. Open the List's **Automations** settings, enable Automations, and
   select the **New Match** trigger
3. Go to Account Settings -> Integrations & API -> Add Integration
4. Fill in:
   - Webhook Name: anything descriptive
   - Webhook URL: `https://your-app.up.railway.app/webhook/propertyradar`
   - Webhook Secret: the exact same string you set as
     `PROPERTYRADAR_WEBHOOK_SECRET` in Railway
5. Send a test webhook if PropertyRadar offers that option, then check
   Railway's logs (Railway dashboard -> your service -> Logs) — the
   first request will print all incoming headers, which tells us
   exactly which header name carries the secret, so we can confirm the
   verification code is checking the right one.

## Testing it

- `GET https://your-app.up.railway.app/health` should return `{"status": "ok"}`
- After a real property triggers the webhook, check your Supabase table
  (Table Editor in the Supabase dashboard) for a new row
- Download the export anytime at:
  `https://your-app.up.railway.app/export.csv?token=YOUR_EXPORT_TOKEN`

## Cost notes

- Every webhook that fires calls PropertyRadar's transactions endpoint
  with `Purchase=1` — this spends real money on your PropertyRadar
  balance every single time, same as the batch script. Budget for
  ongoing per-property cost, not just a one-time pull.
- Railway: this is a lightweight always-on service; expect it to fall
  well within Railway's smallest paid tier for typical webhook volume.
- Supabase: free tier covers this easily unless the table grows into
  the hundreds of thousands of rows.

## Known open item

The exact HTTP header PropertyRadar uses to send the Webhook Secret
isn't documented publicly. `main.py` currently checks a few likely
header names (`X-Webhook-Secret`, `X-PropertyRadar-Secret`,
`Authorization`) and logs all headers on every request so this can be
confirmed against a real test payload and tightened up.
