# Databricks notebook source
# MAGIC %md
# MAGIC # 03_youtube_mentions_backfill
# MAGIC One-time backfill that searches YouTube for videos mentioning each of
# MAGIC the 12 companies in `company_master`, and stores the **count, titles,
# MAGIC and links** as a Delta table. Sentiment scoring is deliberately left out
# MAGIC for now (see the `sentiment_score` column, always NULL) — the plan is to
# MAGIC add a scoring pass (e.g. VADER or an LLM classifier) as a follow-up step
# MAGIC once the raw mention data is flowing reliably.
# MAGIC
# MAGIC - Run this notebook **once**, to seed the last ~90 days
# MAGIC - Use `04_youtube_mentions_weekly_update` for ongoing weekly updates
# MAGIC - Requires a YouTube Data API v3 key (see README_Databricks.md → YouTube
# MAGIC   Mentions setup). The API itself is free; usage is metered in daily quota
# MAGIC   units rather than dollars (a `search.list` call costs 100 units against
# MAGIC   a 10,000-unit/day default quota, so 12 companies × 1 call = 1,200 units
# MAGIC   for this whole backfill — well inside the free daily quota).

# COMMAND ----------

# MAGIC %pip install google-api-python-client langdetect --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import datetime as dt
import time

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from langdetect import detect, LangDetectException
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, TimestampType
)

CATALOG = "quantum_portfolio"
SCHEMA = "market_data"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

LOOKBACK_DAYS = 90
RESULTS_PER_TICKER = 50  # max allowed per search.list call

# COMMAND ----------

# MAGIC %md
# MAGIC ## API key — read from Databricks Secrets (never hardcode this)
# MAGIC Set this up once via the Databricks CLI:
# MAGIC ```bash
# MAGIC databricks secrets create-scope quantum_portfolio
# MAGIC databricks secrets put-secret quantum_portfolio youtube_api_key
# MAGIC ```
# MAGIC See README_Databricks.md for the full walkthrough (getting a key from
# MAGIC Google Cloud Console, enabling the YouTube Data API v3, etc.)

# COMMAND ----------

import re

_raw_key = dbutils.secrets.get(scope="quantum_portfolio", key="youtube_api_key")
# .strip() alone only removes whitespace at the ends. Google API keys should
# only ever contain letters, digits, hyphens, and underscores — stripping
# anything else out defends against invisible characters (e.g. a zero-width
# space picked up when copying from a browser) that .strip() won't catch,
# since they aren't classified as whitespace.
YOUTUBE_API_KEY = re.sub(r"[^A-Za-z0-9_\-]", "", _raw_key)
print(f"Raw length: {len(_raw_key)} -> cleaned length: {len(YOUTUBE_API_KEY)} (a standard Google API key is 39 chars)")
if len(_raw_key) != len(YOUTUBE_API_KEY):
    print(f"[INFO] Removed {len(_raw_key) - len(YOUTUBE_API_KEY)} invalid character(s) from the stored key.")
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# COMMAND ----------

master_pdf = spark.table("company_master").toPandas()
# Search all 12 companies now, not just the 4 pure-play tickers. Query
# strategy differs by category — see the note below.
SEARCH_TARGETS = [
    {"company": row["name"], "ticker": row["ticker"], "category": row["category"]}
    for _, row in master_pdf.iterrows()
]
print(f"Searching YouTube for {len(SEARCH_TARGETS)} companies: {[t['company'] for t in SEARCH_TARGETS]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query strategy by category
# MAGIC A plain `"{company} stock"` query works well for small, thinly-covered
# MAGIC companies like IonQ or Rigetti — almost everything that comes back is
# MAGIC quantum-computing-related, because there isn't much else being said about
# MAGIC them on YouTube. But for **Big tech** names (IBM, Google, Microsoft,
# MAGIC NVIDIA), the same query is dominated by unrelated content — earnings
# MAGIC recaps, general market commentary, etc. — since quantum computing is a
# MAGIC tiny fraction of what gets said about these companies.
# MAGIC
# MAGIC To keep the mention data meaningful, Big tech companies are searched with
# MAGIC `"{company} quantum computing"` instead of `"{company} stock"` — this
# MAGIC trades some recall (fewer total videos) for precision (the videos that
# MAGIC do come back are actually about the relevant topic).

# COMMAND ----------

def build_query(company: str, category: str) -> str:
    if category == "Big tech":
        return f"{company} quantum computing"
    return f"{company} stock"

# COMMAND ----------

def week_start(d: dt.date) -> dt.date:
    """Monday of the week containing date d — used to group mentions by week."""
    return d - dt.timedelta(days=d.weekday())


# ISO 3166-1 alpha-2 -> full country name. Power BI's Map visual geocodes
# full names far more reliably than 2-letter codes, so we convert here
# rather than leaving raw codes for Power BI to guess at. Not exhaustive —
# add entries here if you see codes falling through to "Unknown (XX)".
COUNTRY_CODE_TO_NAME = {
    "US": "United States", "JP": "Japan", "KR": "South Korea", "DE": "Germany",
    "GB": "United Kingdom", "CA": "Canada", "AU": "Australia", "FR": "France",
    "IN": "India", "CN": "China", "TW": "Taiwan", "HK": "Hong Kong",
    "SG": "Singapore", "BR": "Brazil", "MX": "Mexico", "ES": "Spain",
    "IT": "Italy", "NL": "Netherlands", "SE": "Sweden", "CH": "Switzerland",
    "RU": "Russia", "ID": "Indonesia", "TH": "Thailand", "VN": "Vietnam",
    "PH": "Philippines", "MY": "Malaysia", "TR": "Turkey", "PL": "Poland",
    "NO": "Norway", "DK": "Denmark", "FI": "Finland", "IE": "Ireland",
    "NZ": "New Zealand", "ZA": "South Africa", "AE": "United Arab Emirates",
    "SA": "Saudi Arabia", "IL": "Israel", "AT": "Austria", "BE": "Belgium",
    "PT": "Portugal", "GR": "Greece", "CZ": "Czechia", "HU": "Hungary",
    "RO": "Romania", "UA": "Ukraine",
}


def country_code_to_name(code: str) -> str:
    if not code:
        return "Unknown"
    return COUNTRY_CODE_TO_NAME.get(code, f"Unknown ({code})")


def detect_title_language(title: str) -> str:
    """Best-effort language detection on the video title. Returns an
    ISO 639-1 code (e.g. 'en', 'ja', 'ko') or 'unknown' if detection fails
    (very short or ambiguous titles sometimes can't be classified)."""
    try:
        return detect(title)
    except LangDetectException:
        return "unknown"


def fetch_channel_countries(youtube_client, channel_ids: list) -> dict:
    """Batch-fetch each channel's self-declared country via channels.list.
    Costs only 1 quota unit per call (up to 50 IDs per call), regardless of
    how many IDs are requested — cheap even for hundreds of channels.
    Channels that haven't set a country come back with no 'country' field."""
    result = {}
    unique_ids = list(dict.fromkeys(channel_ids))  # de-dupe, keep order
    for i in range(0, len(unique_ids), 50):
        batch = unique_ids[i:i + 50]
        try:
            resp = youtube_client.channels().list(part="snippet", id=",".join(batch)).execute()
            for item in resp.get("items", []):
                result[item["id"]] = item.get("snippet", {}).get("country")
        except HttpError as e:
            print(f"[WARN] channels.list batch failed ({e})")
        time.sleep(0.2)
    return result


def search_youtube_mentions(company: str, ticker: str, query: str, published_after: str, max_results: int = 50):
    """Search YouTube for videos matching `query`, published after the given
    ISO-8601 timestamp. Returns a list of row dicts. `ticker` may be None
    for not-yet-public companies (Quantinuum, IQM) — `company` is always
    populated and is what MERGE keys on."""
    rows = []
    try:
        response = youtube.search().list(
            q=query,
            part="snippet",
            type="video",
            order="date",
            publishedAfter=published_after,
            maxResults=max_results,
        ).execute()

        for item in response.get("items", []):
            video_id = item["id"]["videoId"]
            snippet = item["snippet"]
            published_at = dt.datetime.strptime(
                snippet["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=dt.timezone.utc)
            rows.append({
                "company": company,
                "ticker": ticker,
                "video_id": video_id,
                "channel_id": snippet.get("channelId"),
                "title": snippet["title"],
                "channel_title": snippet["channelTitle"],
                "published_at": published_at,
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "thumbnail_url": snippet.get("thumbnails", {}).get("medium", {}).get("url"),
                "week_start": week_start(published_at.date()),
                "search_query": query,
                "detected_language": detect_title_language(snippet["title"]),
                "sentiment_score": None,  # placeholder — scored in a later pass
                "ingestion_ts": dt.datetime.now(dt.timezone.utc),
            })
    except HttpError as e:
        print(f"[WARN] {company}: YouTube API error ({e})")
    return rows

# COMMAND ----------

published_after = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

all_rows = []
for target in SEARCH_TARGETS:
    query = build_query(target["company"], target["category"])
    rows = search_youtube_mentions(target["company"], target["ticker"], query, published_after, RESULTS_PER_TICKER)
    print(f"{target['company']} (query: \"{query}\"): {len(rows)} videos found")
    all_rows.extend(rows)
    time.sleep(0.5)  # be gentle on the API

mentions_pdf = pd.DataFrame(all_rows)
print(f"\nTotal rows: {len(mentions_pdf)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GEO enrichment: channel country
# MAGIC YouTube doesn't expose a video's country directly, but a channel can
# MAGIC (optionally) declare its own country in its public profile. We batch-fetch
# MAGIC that via `channels.list` — 1 quota unit per call regardless of batch size,
# MAGIC so this is cheap even for a few hundred distinct channels. Channels that
# MAGIC never set a country come back as "Unknown".

# COMMAND ----------

if not mentions_pdf.empty:
    country_by_channel = fetch_channel_countries(youtube, mentions_pdf["channel_id"].dropna().tolist())
    mentions_pdf["channel_country_code"] = mentions_pdf["channel_id"].map(country_by_channel)
    mentions_pdf["channel_country"] = mentions_pdf["channel_country_code"].apply(country_code_to_name)
    print(f"Resolved country for {mentions_pdf['channel_country_code'].notna().sum()} / {len(mentions_pdf)} rows")
else:
    mentions_pdf["channel_country_code"] = pd.Series(dtype="object")
    mentions_pdf["channel_country"] = pd.Series(dtype="object")

# COMMAND ----------

schema = StructType([
    StructField("company", StringType()),
    StructField("ticker", StringType()),
    StructField("video_id", StringType()),
    StructField("channel_id", StringType()),
    StructField("title", StringType()),
    StructField("channel_title", StringType()),
    StructField("published_at", TimestampType()),
    StructField("video_url", StringType()),
    StructField("thumbnail_url", StringType()),
    StructField("week_start", DateType()),
    StructField("search_query", StringType()),
    StructField("detected_language", StringType()),
    StructField("channel_country_code", StringType()),
    StructField("channel_country", StringType()),
    StructField("sentiment_score", DoubleType()),
    StructField("ingestion_ts", TimestampType()),
])

if not mentions_pdf.empty:
    # Reorder columns to exactly match the schema's field order by name.
    # spark.createDataFrame(pandas_df, schema=...) matches columns by
    # POSITION, not name — since channel_country_code/channel_country were
    # added as trailing columns after the initial dict-based construction,
    # their physical position didn't match where the schema expects them,
    # which silently misaligned every column after that point (that's what
    # caused the "ingestion_ts -> Arrow Array (string)" error). Reindexing
    # here makes this robust regardless of the order columns were added in.
    mentions_pdf = mentions_pdf.reindex(columns=[f.name for f in schema.fields])
    mentions_sdf = spark.createDataFrame(mentions_pdf, schema=schema)
else:
    mentions_sdf = spark.createDataFrame([], schema=schema)

(mentions_sdf.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("youtube_mentions_weekly"))

display(mentions_sdf.orderBy("company", "published_at"))

# COMMAND ----------

print("YouTube mentions backfill complete.")
display(spark.sql("""
    SELECT company, ticker, week_start, COUNT(*) AS video_count
    FROM youtube_mentions_weekly
    GROUP BY company, ticker, week_start
    ORDER BY company, week_start
"""))

display(spark.sql("""
    SELECT channel_country, COUNT(*) AS video_count, COUNT(DISTINCT channel_id) AS channel_count
    FROM youtube_mentions_weekly
    GROUP BY channel_country
    ORDER BY video_count DESC
"""))

display(spark.sql("""
    SELECT detected_language, COUNT(*) AS video_count
    FROM youtube_mentions_weekly
    GROUP BY detected_language
    ORDER BY video_count DESC
"""))
