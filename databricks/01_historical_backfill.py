# Databricks notebook source
# MAGIC %md
# MAGIC # 01_historical_backfill
# MAGIC One-time backfill notebook that fetches **3 years of history** for a
# MAGIC quantum-computing company watchlist and writes it to Delta Lake tables.
# MAGIC
# MAGIC - Run this notebook **once**, at the start
# MAGIC - Use `02_weekly_incremental_update` for all subsequent weekly updates
# MAGIC - Daily prices go back a full 3 years, but quarterly financials are
# MAGIC   limited to roughly the last 4-5 quarters — that's a limitation of
# MAGIC   yfinance's free tier (Yahoo doesn't expose more history for free).

# COMMAND ----------

# MAGIC %pip install yfinance --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import datetime as dt
import time

import pandas as pd
import yfinance as yf
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — edit to match your environment

# COMMAND ----------

CATALOG = "quantum_portfolio"      # change to hive_metastore etc. if not using Unity Catalog
SCHEMA = "market_data"
LOOKBACK_YEARS = 3

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Company master (static)

# COMMAND ----------

COMPANY_MASTER = [
    {"name": "IonQ",                   "ticker": "IONQ",  "category": "Pure-play quantum", "approach": "Trapped-ion",           "fiscal_year_end_month": 12},
    {"name": "Rigetti Computing",      "ticker": "RGTI",  "category": "Pure-play quantum", "approach": "Superconducting",       "fiscal_year_end_month": 12},
    {"name": "D-Wave Quantum",         "ticker": "QBTS",  "category": "Pure-play quantum", "approach": "Annealing + gate-model","fiscal_year_end_month": 12},
    {"name": "Quantum Computing Inc.", "ticker": "QUBT",  "category": "Pure-play quantum", "approach": "Photonics",             "fiscal_year_end_month": 12},
    {"name": "Quantinuum",             "ticker": None,    "category": "Pure-play quantum", "approach": "Trapped-ion",           "fiscal_year_end_month": 12},
    {"name": "IQM Quantum Computers",  "ticker": None,    "category": "Pure-play quantum", "approach": "Superconducting",       "fiscal_year_end_month": 12},
    {"name": "IBM",                    "ticker": "IBM",   "category": "Big tech",          "approach": "Superconducting",       "fiscal_year_end_month": 12},
    {"name": "Alphabet (Google)",      "ticker": "GOOG",  "category": "Big tech",          "approach": "Superconducting",       "fiscal_year_end_month": 12},
    {"name": "Microsoft",              "ticker": "MSFT",  "category": "Big tech",          "approach": "Topological",           "fiscal_year_end_month": 6},
    {"name": "NVIDIA",                 "ticker": "NVDA",  "category": "Big tech",          "approach": "CUDA-Q (hybrid platform)", "fiscal_year_end_month": 1},
    {"name": "SEALSQ",                 "ticker": "LAES",  "category": "Post-quantum security", "approach": "Quantum-resistant semiconductors / PKI", "fiscal_year_end_month": 12},
    {"name": "BTQ Technologies",       "ticker": "BTQ",   "category": "Post-quantum security", "approach": "Post-quantum cryptography",              "fiscal_year_end_month": 12},
]

master_df = spark.createDataFrame(COMPANY_MASTER)
master_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("company_master")
display(master_df)

TICKERS = [c["ticker"] for c in COMPANY_MASTER if c["ticker"]]
FYE_MONTH_BY_TICKER = {c["ticker"]: c["fiscal_year_end_month"] for c in COMPANY_MASTER if c["ticker"]}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: derive fiscal year (FY) / fiscal quarter (Q) from a quarter-end date
# MAGIC Companies don't all close their books in December — Microsoft's fiscal year
# MAGIC ends in June, NVIDIA's ends in January. Naively bucketing by calendar month
# MAGIC would misalign them against the calendar-year filers when charted side by
# MAGIC side. This function uses each company's actual fiscal-year-end month to
# MAGIC compute the correct FY label and Q number, so "Q1" always means the same
# MAGIC *relative* position in that company's own fiscal calendar.
# MAGIC
# MAGIC Convention: FY is labeled by the calendar year in which that fiscal year
# MAGIC *ends* (e.g. Microsoft's fiscal year ending June 2026 is "FY2026"; its
# MAGIC quarter ending Sept 30, 2025 is FY2026 Q1 — this matches how these
# MAGIC companies label their own earnings releases).

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
# MAGIC ## 2. Daily prices (3 years) — bulk fetch with `yf.download`
# MAGIC Pulling all tickers at once with `yf.download` is fewer requests and
# MAGIC more stable than looping `Ticker.history()` per symbol.

# COMMAND ----------

end = dt.date.today()
start = end - dt.timedelta(days=365 * LOOKBACK_YEARS)

raw = yf.download(
    tickers=TICKERS,
    start=start.isoformat(),
    end=end.isoformat(),
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

prices_pdf = pd.DataFrame(rows)
print(f"Rows fetched: {len(prices_pdf):,} across {prices_pdf['ticker'].nunique()} tickers")

# COMMAND ----------

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

prices_sdf = spark.createDataFrame(prices_pdf, schema=schema)

(prices_sdf.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("stock_prices_daily"))

display(prices_sdf.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quarterly financials (whatever history is available)

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

fin_pdf = pd.DataFrame(fin_rows)
print(f"Rows fetched: {len(fin_pdf):,} (avg {len(fin_pdf)/max(len(TICKERS),1):.1f} quarters per ticker)")

if not fin_pdf.empty:
    fin_sdf = spark.createDataFrame(fin_pdf)
    fin_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("fundamentals_quarterly")
    display(fin_sdf.orderBy("ticker", "fiscal_quarter_end"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Static tables (market size forecast, segment share)

# COMMAND ----------

market_size = [
    {"source": "Fortune Business Insights", "source_url": "https://www.fortunebusinessinsights.com/quantum-computing-market-104855", "base_year": 2025, "base_value_b": 1.39, "final_year": 2034, "final_value_b": 17.89, "cagr_pct": 33.0},
    {"source": "Grand View Research",        "source_url": "https://www.grandviewresearch.com/industry-analysis/quantum-computing-market", "base_year": 2025, "base_value_b": 1.60, "final_year": 2033, "final_value_b": 8.00,  "cagr_pct": 22.3},
    {"source": "Precedence Research",        "source_url": "https://www.precedenceresearch.com/quantum-computing-market", "base_year": 2025, "base_value_b": 1.44, "final_year": 2035, "final_value_b": 19.44, "cagr_pct": 29.73},
    {"source": "Market.us",                  "source_url": "https://market.us/report/quantum-computing-market/", "base_year": 2025, "base_value_b": 2.20, "final_year": 2035, "final_value_b": 50.40, "cagr_pct": 37.0},
    {"source": "SNS Insider",                "source_url": "https://www.snsinsider.com/reports/quantum-computing-market-2740", "base_year": 2025, "base_value_b": 1.47, "final_year": 2035, "final_value_b": 18.91, "cagr_pct": 29.1},
]
spark.createDataFrame(market_size).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("market_size_forecast")

_seg_src = "Grand View Research"
_seg_url = "https://www.grandviewresearch.com/industry-analysis/quantum-computing-market"
segment_share = [
    {"dimension": "Offering",    "segment": "Systems",                  "share_pct": 63.5, "source": _seg_src, "source_url": _seg_url},
    {"dimension": "Offering",    "segment": "Services",                 "share_pct": 36.5, "source": _seg_src, "source_url": _seg_url},
    {"dimension": "Deployment",  "segment": "On-premise",               "share_pct": 48.4, "source": _seg_src, "source_url": _seg_url},
    {"dimension": "Deployment",  "segment": "Cloud",                    "share_pct": 51.6, "source": _seg_src, "source_url": _seg_url},
    {"dimension": "Application", "segment": "Optimization",             "share_pct": 29.3, "source": _seg_src, "source_url": _seg_url},
    {"dimension": "Industry",    "segment": "BFSI (finance/insurance)", "share_pct": 21.7, "source": _seg_src, "source_url": _seg_url},
]
spark.createDataFrame(segment_share).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("segment_share")

# Sentiment is a manually-maintained table — just create the empty schema for now.
sentiment_schema = StructType([
    StructField("date", DateType()),
    StructField("ticker", StringType()),
    StructField("sentiment_score", DoubleType()),
    StructField("sentiment_label", StringType()),
    StructField("mention_volume", StringType()),
    StructField("source", StringType()),
])
spark.createDataFrame([], sentiment_schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("sentiment_weekly")

print("Historical backfill complete. Tables created:")
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
