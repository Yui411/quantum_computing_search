# Databricks + Power BI setup

## Architecture overview

```
[yfinance] --(one-time)--> 01_historical_backfill.py --> Delta Table (3 years back)
[yfinance] --(weekly)----> 02_weekly_incremental_update.py --(MERGE)--> same Delta Table
                                                                              |
                                                                        Power BI (Import)
```

Tables created (catalog: `quantum_portfolio` / schema: `market_data`):

| Table | Contents | Update frequency |
|---|---|---|
| `company_master` | Company name, ticker, category, technology approach, fiscal-year-end month | One-time (editable by hand) |
| `stock_prices_daily` | Daily OHLCV (3 years and counting) | **Weekly** (via the scheduled job) |
| `fundamentals_quarterly` | Quarterly revenue + `fiscal_year`/`fiscal_quarter`/`fiscal_label` (see below) | **Weekly** (via the scheduled job) |
| `market_size_forecast` | Market size forecasts by research firm, with `source`/`source_url` columns | **Static** — written once by `01_historical_backfill.py`; the weekly job does NOT touch this table |
| `segment_share` | Segment share, with `source`/`source_url` columns | **Static** — same as above, weekly job does NOT touch this table |
| `sentiment_weekly` | Social sentiment | Weekly (empty placeholder rows are auto-created; the actual scores are entered by hand) |
| `youtube_mentions_weekly` | YouTube videos mentioning each company: title, channel, publish date, link, `channel_country` / `detected_language` for GEO breakdowns, plus `sentiment_label` / `matched_keywords` from keyword-based scoring | **Weekly** (via the scheduled job) — covers all 12 companies; sentiment fields filled in by `05_youtube_sentiment_scoring.py`, run separately (see below) |

> **Note:** Only `stock_prices_daily`, `fundamentals_quarterly`, and the
> placeholder rows in `sentiment_weekly` are refreshed by
> `02_weekly_incremental_update.py`. `market_size_forecast` and
> `segment_share` are one-time static tables — if the underlying research
> reports get updated, you'll need to re-run (the relevant cells of)
> `01_historical_backfill.py` by hand, or add a small script that
> overwrites just those two tables.

---

## Fiscal year alignment (FY / Q columns)

Not every company closes its books in December — Microsoft's fiscal year
ends in June, NVIDIA's ends in January. If you chart `fiscal_quarter_end`
directly, off-calendar filers land on different x-axis positions than
calendar-year filers even when comparing "the same" quarter, which is
exactly the misalignment you'll see if you drop NVIDIA into a chart with
IonQ, IBM, etc.

`fundamentals_quarterly` now includes three extra columns computed from
each company's actual fiscal-year-end month (stored in
`company_master.fiscal_year_end_month`):

| Column | Example | Meaning |
|---|---|---|
| `fiscal_year` | `2026` | The calendar year the fiscal year *ends* in — matches how each company labels its own earnings releases (e.g. Microsoft's quarter ending Sept 30, 2025 is `fiscal_year = 2026`) |
| `fiscal_quarter` | `1` | 1-4, the company's own fiscal quarter number |
| `fiscal_label` | `"FY2026 Q1"` | Ready-to-use display label |

In Power BI, use `fiscal_label` (or `fiscal_year` + `fiscal_quarter`
combined) as the x-axis category instead of the raw `fiscal_quarter_end`
date — this aligns all companies by their *relative* position in the
fiscal calendar rather than the literal calendar date. Keep
`fiscal_quarter_end` available in the tooltip so the actual reporting
date is never hidden.

Current fiscal-year-end months in `company_master`:

| Fiscal year end | Companies |
|---|---|
| December | IonQ, Rigetti, D-Wave, QUBT, Quantinuum*, IQM*, IBM, Alphabet, SEALSQ, BTQ |
| June | Microsoft |
| January | NVIDIA |

\* Quantinuum and IQM are not yet covered by yfinance (no ticker), so
December is a placeholder assumption — update it once real fiscal-year
data is available for them.

---

---

## YouTube Mentions setup

`03_youtube_mentions_backfill.py` and `04_youtube_mentions_weekly_update.py`
use the YouTube Data API v3 to find videos that mention each of the 12
companies in `company_master` — capturing video count, title, channel,
publish date, and a direct link. This is Phase 1: raw mention data only.
Sentiment scoring is deliberately deferred to a later phase (see "What's
not done yet" below).

### Query strategy differs by category — read this before running

A plain `"{company} stock"` query works well for small, thinly-covered
companies (IonQ, Rigetti, D-Wave, QUBT, SEALSQ, BTQ, Quantinuum, IQM) —
almost everything that comes back is genuinely about that company, since
there isn't much else being said about them on YouTube.

For **Big tech** companies (IBM, Alphabet, Microsoft, NVIDIA), the same
query is dominated by unrelated content — earnings recaps, general market
commentary, etc. — because quantum computing is a tiny fraction of what
gets said about these companies on YouTube. To keep the data meaningful,
Big tech companies are searched with `"{company} quantum computing"`
instead, trading some recall (fewer total videos) for precision (the
videos that do come back are actually on-topic). This is handled
automatically by the `build_query()` function based on each company's
`category` in `company_master` — no manual intervention needed, but worth
knowing about when you're looking at why IBM has far fewer mentions than
IonQ despite being a much bigger company overall.

### Quota impact of searching all 12 companies

Each company costs one `search.list` call = 100 quota units. 12 companies
× 100 units = **1,200 units per run**, against a 10,000-unit/day free
quota — comfortable headroom even if both the backfill and weekly job
happened to run on the same day.

### Getting an API key (free)

1. Go to [Google Cloud Console](https://console.cloud.google.com/) and create a project (or reuse an existing one)
2. **APIs & Services** → **Library** → search "YouTube Data API v3" → **Enable**
3. **APIs & Services** → **Credentials** → **Create Credentials** → **API key**
4. (Recommended) Restrict the key to only the YouTube Data API v3, to limit blast radius if it ever leaks
5. No billing account or credit card is required — the API has no dollar cost,
   only a daily quota (10,000 units/day by default). See "Quota impact" above
   for the math on searching all 12 companies.

### Storing the key in Databricks Secrets

Never hardcode the API key in a notebook. Use the Databricks CLI:

```bash
databricks secrets create-scope quantum_portfolio
databricks secrets put-secret quantum_portfolio youtube_api_key
# (paste the key when prompted)
```

Both notebooks read it via:
```python
YOUTUBE_API_KEY = dbutils.secrets.get(scope="quantum_portfolio", key="youtube_api_key")
```

### Running it

1. Run `03_youtube_mentions_backfill.py` once (pulls the last 90 days)
2. Add `04_youtube_mentions_weekly_update.py` as a second task in the same
   Databricks Job as `02_weekly_incremental_update.py` (or its own job on
   the same weekly schedule) — it pulls the last 10 days and upserts

### What's not done yet: sentiment scoring

Update: this is now done -- see `05_youtube_sentiment_scoring.py` below.
It's kept as a separate notebook from the collection jobs (`03`/`04`) on
purpose, so mention-collection stays simple and reliable even as scoring
logic evolves independently.

---

## Sentiment scoring (`05_youtube_sentiment_scoring.py`)

Fills in `sentiment_label`, `sentiment_score`, and `matched_keywords` for
any row where `sentiment_label IS NULL`. Run it once after `03`/`04` have
populated some mentions, and optionally add it as a third task in the same
weekly Databricks Job so new mentions get scored automatically.

### Method: keyword dictionary, not a trained model

Each title is scanned against a small hand-curated list of bullish/bearish
finance terms across the languages seen so far (English, Japanese, Korean,
German -- see `POSITIVE_KEYWORDS` / `NEGATIVE_KEYWORDS` in the notebook).
This was chosen deliberately over a black-box model: every classification
comes with the **exact keyword(s) that triggered it**
(`matched_keywords`), which is both easier to sanity-check and easier to
extend -- if a title should have matched and didn't, just add the missing
word to the list.

| Column | Example | Meaning |
|---|---|---|
| `sentiment_label` | `"positive"` | One of `positive` / `negative` / `mixed` / `neutral` |
| `sentiment_score` | `2.0` | Positive keyword count minus negative keyword count |
| `matched_keywords` | `"surge, buy"` | The actual keyword(s) found, comma-separated |

`"neutral"` means "no sentiment keyword detected" -- not a confirmed
neutral tone. A title with no finance jargon at all (e.g. a general
explainer video) will also land here.

### Known limitations

- Substring matching only -- no negation handling (a title containing "not
  bullish" still matches "bullish" as positive)
- No sarcasm/irony detection
- The keyword lists are a starting point, not exhaustive -- expect to add
  to them over time as new phrasing shows up in real titles
- Scans against all languages' keywords regardless of `detected_language`,
  since language detection on short titles isn't perfectly reliable and
  checking everything is cheap

### Why the MERGE only touches unscored rows

Same reasoning as the country/language fields: if you ever manually
correct a `sentiment_label` (e.g. after reviewing a `"mixed"` result), a
future run of this notebook won't overwrite it, because the `WHERE
sentiment_label IS NULL` filter skips any row that already has a value.

### Power BI notes (sentiment)

- A **stacked bar** of `sentiment_label` by `company` (or by `week_start`)
  gives an at-a-glance bullish/bearish split per ticker over time
- A **table** filtered to `matched_keywords != ''`, sorted by
  `published_at` descending, works well as a "what's driving the
  sentiment" drill-down next to the chart
- Pair with the existing `video_url` (Web URL data category) so a person
  can click straight from "this ticker turned negative this week" to the
  actual videos

---

### Power BI notes

- `video_url` can be set as a **Web URL** data category in Power BI's
  modeling view, which turns it into a clickable link in table visuals
- `thumbnail_url` can similarly be set as an **Image URL**, letting you
  show video thumbnails directly in a table or card visual
- A simple **video count by ticker by week** bar chart
  (`COUNT(video_id)` grouped by `ticker`, `week_start`) already answers
  "how many videos mentioned IonQ this week" without needing sentiment
  scoring at all

### GEO / language enrichment

Two extra columns add a geographic and linguistic dimension to the
mention data, both filled in automatically — no manual work required:

| Column | How it's derived | Caveats |
|---|---|---|
| `channel_country` | The channel's self-declared country (via `channels.list`), mapped from a 2-letter code to a full name for Power BI map geocoding. Falls back to `"Unknown"` if the channel never set one. | Many channels don't set this — expect a large "Unknown" share. This is a *channel* attribute, not a *video* attribute; a channel based in the US can still post in Korean, etc. |
| `detected_language` | Detected from the video **title only** (via `langdetect`), as an ISO 639-1 code (`en`, `ja`, `ko`, `de`, ...) or `"unknown"` if detection fails on very short/ambiguous text | Titles are short, so detection is a best-effort signal, not a certainty — don't treat single-video results as authoritative, but it's reliable in aggregate across many videos |

In Power BI:
- **Map visual** using `channel_country` as the location field (the
  Map/Filled Map visuals both geocode full country names well) —
  `COUNT(video_id)` as the size/color measure gives an at-a-glance view of
  where mention volume is concentrated
- **Bar or pie chart** of `detected_language` — pairs well with a
  ticker/company slicer to see, e.g., "is IonQ getting more Korean-
  language coverage than Rigetti this month?"
- Both columns are on the same `youtube_mentions_weekly` table as
  `title`/`video_url`, so a table visual filtered by country or language
  can drill straight into the actual videos

Quota cost of the country lookup: `channels.list` costs 1 unit per call
regardless of batch size (up to 50 channel IDs per call), so even a few
hundred distinct channels only add a handful of quota units — negligible
next to the 100-unit `search.list` calls.

---

## Step 1: Initial setup

1. Import `databricks/01_historical_backfill.py` and
   `databricks/02_weekly_incremental_update.py` into your Databricks
   workspace (**Workspace** → **Import**, then upload the files)
2. Attach a cluster and run `01_historical_backfill.py` once
   - This assumes Unity Catalog is enabled. If it isn't, change
     `CATALOG = "quantum_portfolio"` in the notebook to `hive_metastore`
     or similar
   - After running, you'll have 6 tables in the `quantum_portfolio.market_data` schema

---

## Step 2: Schedule the weekly update as a Databricks Job

1. Left sidebar **Workflows** → **Create Job**
2. Task settings:
   - **Type**: Notebook
   - **Path**: `02_weekly_incremental_update.py`
   - **Cluster**: a new job cluster (a small instance is plenty)
3. **Add trigger**:
   - Trigger type: **Scheduled**
   - Example: every Monday at 6:00 AM (before markets open is a good choice)
   - Example cron expression: `0 0 6 ? * MON`
4. Optionally add a second task in the same job running
   `04_youtube_mentions_weekly_update.py` (see "YouTube Mentions setup"
   above) so both updates run on the same schedule
5. Set up failure notifications (email/Slack) for peace of mind
6. Save, then click **Run now** once to test manually

From here on, the last 10 days of prices are MERGEd in automatically every
week, with no gaps or duplicates.

---

## Step 3: Connect Power BI to Databricks

Connecting to Databricks is a **cloud connector** in Power BI, so no
on-premises data gateway is needed.

1. In Databricks, get your connection details:
   **Compute** → the SQL warehouse you're using → **Connection details** tab
   - Server hostname
   - HTTP path
2. Generate a **Personal Access Token (PAT)** for authentication:
   top-right user icon → **Settings** → **Developer** → **Access tokens**
   → **Generate new token**
3. Power BI Desktop → **Get Data** → **Azure Databricks** (search "Databricks" in the connector list)
4. Enter the server hostname and HTTP path → choose **Personal Access
   Token** as the authentication method and paste in the token from step 2
5. Check the 6 tables under `quantum_portfolio.market_data` and load them
6. **Import mode is recommended** (it matches the weekly update cadence).
   Set Power BI Service's own scheduled refresh to weekly as well, so it
   automatically picks up new data after the Databricks job finishes
   (schedule the Power BI refresh a bit later than the Databricks job's
   completion time)

---

## Power BI modeling tips

- Relate `stock_prices_daily` and `company_master` on the `ticker` column (one-to-many)
- Create a separate date table (e.g. via the `CALENDAR` DAX function) and
  relate it to `stock_prices_daily[date]` — this makes date slicers and
  year/quarter drill-downs much easier
- `fundamentals_quarterly` will have a different number of quarters
  available per company (roughly the last 4-5). If you extend a time-series
  chart back further, expect gaps for some tickers
- `stock_prices_daily` contains price data only (OHLCV) — no market cap
  column, by design (see "Known limitations" below)

---

## Known limitations (being upfront about these)

1. **Quarterly financials don't go back 3 years** — yfinance's free tier
   caps out at roughly the last 4-5 quarters. If you need a full 3 years of
   quarterly financials, you'll need to switch to the SEC EDGAR API
   (10-Q/10-K filings) or a paid data vendor (Alpha Vantage's paid tier,
   Finnhub, etc.)
2. **No historical market cap is stored** — since a full, accurate history
   of shares outstanding isn't available for free, we deliberately left
   this out rather than ship an error-prone approximation.
   `stock_prices_daily` is price data (OHLCV) only.
3. **Sentiment isn't automated** — no free API with historical social-
   listening data was found, so this pipeline only auto-creates an empty
   row each week; the actual score is entered by hand. For ongoing
   automation, consider a paid service like AltIndex or the Stocktwits API.
