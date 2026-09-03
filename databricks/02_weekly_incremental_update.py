# Databricks notebook source
# MAGIC %md
# MAGIC # 02_weekly_incremental_update
# MAGIC Weekly incremental-update notebook, meant to be **scheduled as a Databricks Job**.
# MAGIC
# MAGIC Fetches the last 10 days of prices plus the latest quarterly financials
# MAGIC and upserts them into the existing Delta tables with `MERGE INTO`
# MAGIC (idempotent — safe to re-run). Pulling 10 days instead of 7 gives a
# MAGIC safety margin so that one or two missed/failed runs still get caught up.
# MAGIC
# MAGIC Sentiment (`sentiment_weekly`) has no automated data source, so this
# MAGIC notebook only creates this week's empty placeholder rows at the end.
# MAGIC Fill in the actual scores by checking Stocktwits / AltIndex etc.
# MAGIC (either via the Databricks SQL editor with an `UPDATE`, or by editing
# MAGIC the placeholder cell below directly).

# COMMAND ----------

# MAGIC %pip install yfinance --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import datetime as dt
import time

import pandas as pd
import yfinance as yf
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType, TimestampType
)

CATALOG = "quantum_portfolio"
SCHEMA = "market_data"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

RECENT_DAYS = 10  # overlap window to avoid gaps from missed runs

# COMMAND ----------

master_pdf = spark.table("company_master").toPandas()
TICKERS = [t for t in master_pdf["ticker"].tolist() if t]
FYE_MONTH_BY_TICKER = dict(zip(master_pdf["ticker"], master_pdf["fiscal_year_end_month"]))
print(f"Updating {len(TICKERS)} tickers -> {TICKERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: derive fiscal year (FY) / fiscal quarter (Q) from a quarter-end date
# MAGIC Same logic as in `01_historical_backfill.py` — kept in sync so FY/Q labels
# MAGIC don't drift between the initial backfill and the weekly updates. Each
# MAGIC company's own fiscal-year-end month (from `company_master`) is used so that
# MAGIC "Q1" always means the same relative position in that company's fiscal
# MAGIC calendar, even for off-calendar filers like Microsoft (June) or NVIDIA (January).

# COMMAND ----------

def fiscal_year_and_quarter(period_end, fye_month: int):
    """Given a quarter-end date and the company's fiscal-year-end month (1-12),
    return (fiscal_year, fiscal_quarter) as (int, int)."""
    fy = period_end.year if period_end.month <= fye_month else period_end.year + 1
    fy_start_month = (fye_month % 12) + 1
    months_since_start = (period_end.month - fy_start_month) % 12
    q = months_since_start // 3 + 1
    return fy, q

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fetch recent prices and MERGE

# COMMAND ----------

raw = yf.download(
    tickers=TICKERS,
    period=f"{RECENT_DAYS}d",
    group_by="ticker",
    auto_adjust=False,
    threads=True,
)

# COMMAND ----------

rows = []
for t in TICKERS:
    try:
        df_t = raw[t].reset_index()
    except KeyError:
        print(f"[WARN] {t}: no price data")
        continue
    df_t = df_t.dropna(subset=["Close"])
    for _, r in df_t.iterrows():
        rows.append({
            "ticker": t,
            "date": r["Date"].date(),
            "open": float(r["Open"]) if pd.notna(r["Open"]) else None,
            "high": float(r["High"]) if pd.notna(r["High"]) else None,
            "low": float(r["Low"]) if pd.notna(r["Low"]) else None,
            "close": float(r["Close"]) if pd.notna(r["Close"]) else None,
            "volume": int(r["Volume"]) if pd.notna(r["Volume"]) else None,
            "ingestion_ts": dt.datetime.now(dt.timezone.utc),
        })

update_pdf = pd.DataFrame(rows)
print(f"Fetched this run: {len(update_pdf)} rows")

schema = StructType([
    StructField("ticker", StringType()),
    StructField("date", DateType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
    StructField("ingestion_ts", TimestampType()),
])
update_sdf = spark.createDataFrame(update_pdf, schema=schema)
update_sdf.createOrReplaceTempView("stock_prices_update")

# COMMAND ----------

spark.sql("""
MERGE INTO stock_prices_daily AS target
USING stock_prices_update AS source
ON target.ticker = source.ticker AND target.date = source.date
WHEN MATCHED THEN UPDATE SET
    target.open = source.open,
    target.high = source.high,
    target.low = source.low,
    target.close = source.close,
    target.volume = source.volume,
    target.ingestion_ts = source.ingestion_ts
WHEN NOT MATCHED THEN INSERT *
""")

print("stock_prices_daily updated.")
display(spark.sql("SELECT ticker, MAX(date) AS latest_date, COUNT(*) AS n_rows FROM stock_prices_daily GROUP BY ticker ORDER BY ticker"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Incremental update for quarterly financials

# COMMAND ----------

fin_rows = []
for t in TICKERS:
    try:
        q = yf.Ticker(t).quarterly_income_stmt
        if q is not None and "Total Revenue" in q.index:
            rev_row = q.loc["Total Revenue"]
            fye_month = FYE_MONTH_BY_TICKER.get(t, 12)
            for period_end, value in rev_row.items():
                if pd.isna(value):
                    continue
                p_end = period_end.date() if hasattr(period_end, "date") else period_end
                fy, fq = fiscal_year_and_quarter(p_end, fye_month)
                fin_rows.append({
                    "ticker": t,
                    "fiscal_quarter_end": p_end,
                    "fiscal_year": fy,
                    "fiscal_quarter": fq,
                    "fiscal_label": f"FY{fy} Q{fq}",
                    "total_revenue_usd": float(value),
                    "ingestion_ts": dt.datetime.now(dt.timezone.utc),
                })
    except Exception as e:
        print(f"[WARN] {t}: failed to fetch quarterly financials ({e})")
    time.sleep(0.3)

if fin_rows:
    fin_update_sdf = spark.createDataFrame(pd.DataFrame(fin_rows))
    fin_update_sdf.createOrReplaceTempView("fundamentals_update")
    spark.sql("""
    MERGE INTO fundamentals_quarterly AS target
    USING fundamentals_update AS source
    ON target.ticker = source.ticker AND target.fiscal_quarter_end = source.fiscal_quarter_end
    WHEN MATCHED THEN UPDATE SET
        target.fiscal_year = source.fiscal_year,
        target.fiscal_quarter = source.fiscal_quarter,
        target.fiscal_label = source.fiscal_label,
        target.total_revenue_usd = source.total_revenue_usd,
        target.ingestion_ts = source.ingestion_ts
    WHEN NOT MATCHED THEN INSERT *
    """)
    print("fundamentals_quarterly updated.")
else:
    print("No new quarterly financial data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sentiment — create this week's empty rows (manual entry)

# COMMAND ----------

today = dt.date.today()
pure_play_tickers = master_pdf.loc[master_pdf["category"] == "Pure-play quantum", "ticker"].dropna().tolist()

placeholder_rows = [
    {"date": today, "ticker": t, "sentiment_score": None, "sentiment_label": None,
     "mention_volume": None, "source": "Pending manual update — check Stocktwits/AltIndex etc."}
    for t in pure_play_tickers
]
placeholder_sdf = spark.createDataFrame(
    pd.DataFrame(placeholder_rows),
    schema=StructType([
        StructField("date", DateType()),
        StructField("ticker", StringType()),
        StructField("sentiment_score", DoubleType()),
        StructField("sentiment_label", StringType()),
        StructField("mention_volume", StringType()),
        StructField("source", StringType()),
    ])
)

# Don't duplicate rows if this week's date already has entries
existing = spark.sql(f"SELECT date, ticker FROM sentiment_weekly WHERE date = '{today.isoformat()}'")
if existing.count() == 0:
    placeholder_sdf.write.mode("append").saveAsTable("sentiment_weekly")
    print(f"Added empty sentiment rows for {today}. Fill in scores manually.")
else:
    print(f"Sentiment rows for {today} already exist. Skipped.")

# COMMAND ----------

print("Weekly update complete:", dt.datetime.now(dt.timezone.utc).isoformat())
