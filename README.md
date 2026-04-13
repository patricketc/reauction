# LA County Auction Property Tracker

A personal research tool for investigating real estate going to public auction by the
Los Angeles County Treasurer-Tax Collector. It builds a filterable, searchable table
plus a map of every parcel in the auction book, enriched with data from the LA County
Assessor portal and tax-default status from the TTC.

## What it does

1. **Parses** the LA County TTC auction book PDF into structured rows (AIN, minimum bid,
   item number, situs address when present).
2. **Enriches** each parcel by looking up its AIN on `portal.assessor.lacounty.gov`
   (use code, use description, lot size, year built, land/improvement value, situs).
3. **Checks default status** against the TTC property-tax portal so you can see which
   parcels have been redeemed and pulled from the auction.
4. **Geocodes** each address via OpenStreetMap Nominatim.
5. **Serves** a static web UI (Leaflet map + filterable table).

All network-hitting steps are rate-limited (1 req/sec by default) and cached per-AIN
to a JSON file on disk, so re-runs are cheap and respectful to the source sites.

## Layout

```
reauction/
├── pipeline/              # Python data pipeline
│   ├── parse_pdf.py       # PDF -> [{ain, min_bid, item_no, ...}]
│   ├── enrich_assessor.py # AIN -> assessor details (cached)
│   ├── check_default.py   # AIN -> current default status (cached)
│   ├── geocode.py         # address -> lat/lng (cached)
│   ├── use_codes.py       # Assessor use code -> category mapping
│   └── run.py             # Orchestrator; produces data/properties.json
├── data/
│   ├── 2026A-Auction-Book.pdf  (you download this)
│   └── cache/             # per-AIN JSON cache (gitignored)
└── web/
    ├── index.html
    ├── app.js
    ├── styles.css
    ├── sample.json        # tiny sample dataset for UI preview
    └── properties.json    # produced by the pipeline (gitignored)
```

## Quick start

```bash
# 1. Install deps
pip install -r requirements.txt

# 2. Download the auction book
curl -o data/2026A-Auction-Book.pdf \
  https://ttc.lacounty.gov/wp-content/uploads/2026/03/2026A-Auction-Book.pdf

# 3. Run the pipeline (takes a while because of rate limiting)
python -m pipeline.run --pdf data/2026A-Auction-Book.pdf

# 4. Serve the UI locally
python -m http.server 8000 --directory web
# then open http://localhost:8000
```

The UI auto-falls back to `web/sample.json` if `web/properties.json` doesn't
exist yet, so step 4 works immediately — you'll see sample data while the
pipeline runs in another terminal.

## Running incrementally

The pipeline caches every AIN lookup at `data/cache/<AIN>.json`, so a second run only
fetches rows that are new or stale. To force a re-fetch of a single parcel just delete
its cache file.

Useful flags:

```
python -m pipeline.run \
  --pdf data/2026A-Auction-Book.pdf \
  --out web/properties.json \
  --limit 25                  # only process the first N parcels (for testing)
  --skip-default              # skip the TTC default-status check
  --skip-geocode              # skip geocoding
  --rate 1.0                  # seconds between requests (default 1.0)
```

## Hosting on GitHub Pages

You don't need to run your own server. Everything under `web/` is static,
and the pipeline can run in GitHub Actions on a schedule, so the whole
thing lives entirely on GitHub.

Two workflows in `.github/workflows/` do the work:

| Workflow | What it does | When it runs |
| --- | --- | --- |
| `update-data.yml` | Runs the Python pipeline, commits `web/properties.json` back to `main`. | Weekly (Mon 13:00 UTC) + manual |
| `deploy-pages.yml` | Uploads `web/` as a Pages artifact and deploys it. | Push to `main` touching `web/`, after `update-data.yml` succeeds, or manual |

### First-time setup

1. Push this repo to GitHub.
2. In the repo's **Settings → Pages**, set **Source** to **GitHub Actions**.
3. In **Settings → Actions → General → Workflow permissions**, make sure
   "Read and write permissions" is enabled (so `update-data.yml` can push
   the refreshed `properties.json` back to `main`).
4. Go to the **Actions** tab, pick **Update auction data**, and click
   **Run workflow**. On the first run the per-AIN cache is empty, so expect
   the job to take a while (1 req/sec against the assessor + TTC).
   - Tip: for a quick smoke test, set `limit` to something small (e.g. `25`).
5. When `update-data.yml` finishes it commits `web/properties.json` to
   `main`, which triggers `deploy-pages.yml`. Your site will show up at
   `https://<your-username>.github.io/reauction/`.

### Refreshing the data

- **Automatic:** the scheduled run re-fetches weekly. The per-AIN cache is
  persisted across runs via `actions/cache`, so only new/changed parcels
  hit the network.
- **On demand:** Actions tab → *Update auction data* → *Run workflow*.

### Things to tweak

- **Schedule.** Edit the `cron:` in `update-data.yml`. Closer to auction
  day you probably want it daily; off-season, monthly is plenty.
- **PDF URL.** The workflow defaults to the 2026A book URL from the README.
  For a new auction, either edit the default in `update-data.yml` or pass
  a different URL when dispatching the workflow manually.
- **Rate limit.** Stays at 1 req/sec by default. Please don't lower it —
  these are public county services.

### Known limitations on Pages

- GitHub Pages only serves static files, so every browser visit reads the
  same `properties.json` that the last pipeline run produced. The "in
  default / redeemed" status is only as fresh as the last CI run — bump
  the schedule (or run it manually) in the days before an auction.
- GitHub Actions jobs have a 6-hour cap. The pipeline comfortably fits
  under that with the cache warm, but the very first run on a fresh auction
  book can be slow. If you hit the cap, re-running the workflow will pick up
  from the cached rows.

## Notes & caveats

- **Source structure assumptions.** The PDF parser looks for 10-digit AINs and adjacent
  dollar amounts. LA County has used this layout consistently, but if the 2026A book
  changes column order you may need to adjust `pipeline/parse_pdf.py`.
- **Assessor portal scraping.** The portal doesn't advertise a stable API. The enricher
  first tries the public ArcGIS parcel FeatureServer (structured JSON, preferred) and
  falls back to parsing the HTML parcel-detail page. Either path can break if LA County
  redesigns things — both are isolated functions for easy patching.
- **"Still in default" is a moving target.** A parcel can be redeemed right up until
  auction close. Re-run the default-status step close to the auction date for fresh data.
- **Respect the sources.** Defaults are 1 req/sec and per-AIN caching. Please don't
  crank the rate up — these are public services.
- **This is for personal research.** It is not a substitute for title work, a
  professional property inspection, or legal advice before bidding.
