# Databricks notebook source
# MAGIC %md
# MAGIC # 04_youtube_mentions_weekly_update
# MAGIC Weekly incremental update, meant to be **scheduled as a Databricks Job**
# MAGIC alongside `02_weekly_incremental_update`.
# MAGIC
# MAGIC Searches the last 10 days (overlap window, same pattern as the price
# MAGIC update job) for videos mentioning each of the 12 companies in
# MAGIC `company_master`, and upserts them into `youtube_mentions_weekly` with
# MAGIC `MERGE INTO` keyed on `(company, video_id)` — safe to re-run, no
# MAGIC duplicate rows. `ticker` is carried along but not part of the key, since
# MAGIC Quantinuum/IQM don't have one yet.
# MAGIC
# MAGIC `sentiment_score` stays NULL here too. Scoring is intentionally a
# MAGIC separate, later step (see README_Databricks.md → "YouTube Mentions" for
# MAGIC the plan) so this notebook's only job is reliably capturing count / title
# MAGIC / link data every week.

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

RECENT_DAYS = 10  # overlap window, same reasoning as the price update job
RESULTS_PER_TICKER = 50

# COMMAND ----------

import re

_raw_key = dbutils.secrets.get(scope="quantum_portfolio", key="youtube_api_key")
YOUTUBE_API_KEY = re.sub(r"[^A-Za-z0-9_\-]", "", _raw_key)
print(f"Raw length: {len(_raw_key)} -> cleaned length: {len(YOUTUBE_API_KEY)} (a standard Google API key is 39 chars)")
if len(_raw_key) != len(YOUTUBE_API_KEY):
    print(f"[INFO] Removed {len(_raw_key) - len(YOUTUBE_API_KEY)} invalid character(s) from the stored key.")
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# COMMAND ----------

master_pdf = spark.table("company_master").toPandas()
SEARCH_TARGETS = [
    {"company": row["name"], "ticker": row["ticker"], "category": row["category"]}
    for _, row in master_pdf.iterrows()
]
print(f"Updating YouTube mentions for {len(SEARCH_TARGETS)} companies: {[t['company'] for t in SEARCH_TARGETS]}")

# COMMAND ----------

def build_query(company: str, category: str) -> str:
    """Big tech names need a narrower query (plain '{company} stock' is
    dominated by unrelated finance content); smaller, thinly-covered
    companies don't. See 03_youtube_mentions_backfill.py for the full
    rationale — keep this in sync with that notebook."""
    if category == "Big tech":
        return f"{company} quantum computing"
    return f"{company} stock"

# COMMAND ----------

def week_start(d: dt.date) -> dt.date:
    return d - dt.timedelta(days=d.weekday())


# Keep this table in sync with COUNTRY_CODE_TO_NAME in
# 03_youtube_mentions_backfill.py — same rationale: Power BI's Map visual
# geocodes full country names far more reliably than 2-letter codes.
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
    try:
        return detect(title)
    except LangDetectException:
        return "unknown"


def fetch_channel_countries(youtube_client, channel_ids: list) -> dict:
    """Same approach as the backfill notebook — 1 quota unit per call
    regardless of batch size (up to 50 IDs)."""
    result = {}
    unique_ids = list(dict.fromkeys(channel_ids))
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
                "sentiment_score": None,
                "ingestion_ts": dt.datetime.now(dt.timezone.utc),
            })
    except HttpError as e:
        print(f"[WARN] {company}: YouTube API error ({e})")
    return rows

# COMMAND ----------

published_after = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=RECENT_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

all_rows = []
for target in SEARCH_TARGETS:
    query = build_query(target["company"], target["category"])
    rows = search_youtube_mentions(target["company"], target["ticker"], query, published_after, RESULTS_PER_TICKER)
    print(f"{target['company']} (query: \"{query}\"): {len(rows)} videos found in the last {RECENT_DAYS} days")
    all_rows.extend(rows)
    time.sleep(0.5)

update_pdf = pd.DataFrame(all_rows)
print(f"\nFetched this run: {len(update_pdf)} rows")

# COMMAND ----------

if not update_pdf.empty:
    country_by_channel = fetch_channel_countries(youtube, update_pdf["channel_id"].dropna().tolist())
    update_pdf["channel_country_code"] = update_pdf["channel_id"].map(country_by_channel)
    update_pdf["channel_country"] = update_pdf["channel_country_code"].apply(country_code_to_name)
else:
    update_pdf["channel_country_code"] = pd.Series(dtype="object")
    update_pdf["channel_country"] = pd.Series(dtype="object")

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

if update_pdf.empty:
    print("No new videos found in this window. Nothing to merge.")
else:
    # Same fix as 03_youtube_mentions_backfill.py — reorder columns to match
    # the schema by name, since createDataFrame matches by position.
    update_pdf = update_pdf.reindex(columns=[f.name for f in schema.fields])
    update_sdf = spark.createDataFrame(update_pdf, schema=schema)
    update_sdf.createOrReplaceTempView("youtube_mentions_update")

    # Note: sentiment_score is intentionally excluded from the UPDATE SET below.
    # Once a scoring pass fills it in for a video, we don't want this job to
    # blindly overwrite it back to NULL on the next run.
    spark.sql("""
    MERGE INTO youtube_mentions_weekly AS target
    USING youtube_mentions_update AS source
    ON target.company = source.company AND target.video_id = source.video_id
    WHEN MATCHED THEN UPDATE SET
        target.ticker = source.ticker,
        target.channel_id = source.channel_id,
        target.title = source.title,
        target.channel_title = source.channel_title,
        target.published_at = source.published_at,
        target.video_url = source.video_url,
        target.thumbnail_url = source.thumbnail_url,
        target.week_start = source.week_start,
        target.search_query = source.search_query,
        target.detected_language = source.detected_language,
        target.channel_country_code = source.channel_country_code,
        target.channel_country = source.channel_country,
        target.ingestion_ts = source.ingestion_ts
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("youtube_mentions_weekly updated.")

# COMMAND ----------

display(spark.sql("""
    SELECT company, ticker, week_start, COUNT(*) AS video_count
    FROM youtube_mentions_weekly
    GROUP BY company, ticker, week_start
    ORDER BY company DESC, week_start DESC
"""))

display(spark.sql("""
    SELECT channel_country, COUNT(*) AS video_count
    FROM youtube_mentions_weekly
    GROUP BY channel_country
    ORDER BY video_count DESC
"""))

print("Weekly YouTube mentions update complete:", dt.datetime.now(dt.timezone.utc).isoformat())
