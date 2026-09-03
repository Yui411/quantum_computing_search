# Databricks notebook source
# MAGIC %md
# MAGIC # 05_youtube_sentiment_scoring
# MAGIC Phase 2 of the YouTube mentions pipeline: fills in `sentiment_label`,
# MAGIC `matched_keywords`, and `sentiment_score` for rows that don't have them
# MAGIC yet. This is the follow-up step referenced in `03_youtube_mentions_backfill.py`
# MAGIC and `04_youtube_mentions_weekly_update.py`, which deliberately left
# MAGIC sentiment scoring for later.
# MAGIC
# MAGIC ## Method: keyword dictionary, not a trained model
# MAGIC Each title is scanned against a small hand-curated list of bullish /
# MAGIC bearish finance terms across the languages seen so far (English,
# MAGIC Japanese, Korean, German). This is deliberately simple and transparent —
# MAGIC every classification comes with the exact keyword(s) that triggered it
# MAGIC (`matched_keywords`), rather than an opaque model score.
# MAGIC
# MAGIC **Known limitations:**
# MAGIC - Substring matching only — no negation handling ("not bullish" still
# MAGIC   matches "bullish" as positive)
# MAGIC - No sarcasm/irony detection
# MAGIC - The keyword lists are a starting point, not exhaustive — expand
# MAGIC   `POSITIVE_KEYWORDS` / `NEGATIVE_KEYWORDS` below as you see titles that
# MAGIC   should have matched but didn't
# MAGIC - Titles with no matching keyword are labeled `"neutral"`, which really
# MAGIC   means "no sentiment keyword detected", not "confirmed neutral in tone"
# MAGIC
# MAGIC Safe to re-run — only rows where `sentiment_label IS NULL` are scored,
# MAGIC so it never re-processes (or overwrites a manual correction to) an
# MAGIC already-scored row. Schedule this as a third task alongside `02` and `04`
# MAGIC if you want new mentions scored automatically every week.

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

CATALOG = "quantum_portfolio"
SCHEMA = "market_data"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keyword dictionary
# MAGIC Add to these lists over time — they only cover what's been seen in the
# MAGIC data so far. Keep `POSITIVE_KEYWORDS` and `NEGATIVE_KEYWORDS` in the same
# MAGIC order (language-grouped) so they're easy to extend per-language.

# COMMAND ----------

POSITIVE_KEYWORDS = [
    # English
    "surge", "soar", "rally", "jump", "breakout", "bullish", "buy", "upgrade",
    "record high", "outperform", "gains", "rocket", "skyrocket", "climb", "beat",
    "strong buy", "moon",
    # Japanese
    "急騰", "高騰", "上昇", "買い", "強気", "最高値", "急上昇", "反発", "好調",
    # Korean
    "급등", "상승", "강세", "매수", "호재", "반등", "신고가",
    # German
    "steigt", "hausse", "kaufen", "bullisch", "anstieg", "rekord", "aufwärts",
]

NEGATIVE_KEYWORDS = [
    # English
    "crash", "plunge", "sell-off", "selloff", "bearish", "downgrade", "drop",
    "fall", "tumble", "collapse", "warning", "correction", "dump", "sink",
    "slump", "loss", "miss",
    # Japanese
    "急落", "暴落", "下落", "売り", "弱気", "下降", "低迷", "損失", "警告",
    # Korean
    "급락", "폭락", "하락", "매도", "약세", "악재", "손실", "경고",
    # German
    "fällt", "baisse", "verkaufen", "bärisch", "abwärts", "verlust", "warnung",
]


def score_sentiment(title: str):
    """Scan a title against the keyword lists. Returns
    (label, net_score, matched_keywords_csv). Scans against ALL languages'
    keywords regardless of detected_language — language detection on short
    titles isn't perfectly reliable, and this is cheap to just check everything."""
    if not title:
        return "neutral", 0.0, ""
    t = title.lower()
    pos = [kw for kw in POSITIVE_KEYWORDS if kw.lower() in t]
    neg = [kw for kw in NEGATIVE_KEYWORDS if kw.lower() in t]
    if len(pos) > len(neg):
        label = "positive"
    elif len(neg) > len(pos):
        label = "negative"
    elif pos and neg:
        label = "mixed"
    else:
        label = "neutral"
    return label, float(len(pos) - len(neg)), ", ".join(pos + neg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure the table has the columns this notebook needs
# MAGIC `sentiment_score` already exists (always NULL up to now). `sentiment_label`
# MAGIC and `matched_keywords` are new — added only if not already present, so
# MAGIC this is safe to re-run.

# COMMAND ----------

existing_cols = [f.name for f in spark.table("youtube_mentions_weekly").schema.fields]
cols_to_add = []
if "sentiment_label" not in existing_cols:
    cols_to_add.append("sentiment_label STRING")
if "matched_keywords" not in existing_cols:
    cols_to_add.append("matched_keywords STRING")

if cols_to_add:
    spark.sql(f"ALTER TABLE youtube_mentions_weekly ADD COLUMNS ({', '.join(cols_to_add)})")
    print(f"Added columns: {cols_to_add}")
else:
    print("sentiment_label / matched_keywords already exist — nothing to add.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score unscored rows only

# COMMAND ----------

unscored_pdf = spark.sql("""
    SELECT company, video_id, title
    FROM youtube_mentions_weekly
    WHERE sentiment_label IS NULL
""").toPandas()

print(f"Rows to score: {len(unscored_pdf)}")

if not unscored_pdf.empty:
    results = unscored_pdf["title"].apply(score_sentiment)
    unscored_pdf["sentiment_label"] = results.apply(lambda r: r[0])
    unscored_pdf["sentiment_score"] = results.apply(lambda r: r[1])
    unscored_pdf["matched_keywords"] = results.apply(lambda r: r[2])

    update_sdf = spark.createDataFrame(
        unscored_pdf[["company", "video_id", "sentiment_label", "sentiment_score", "matched_keywords"]],
        schema=StructType([
            StructField("company", StringType()),
            StructField("video_id", StringType()),
            StructField("sentiment_label", StringType()),
            StructField("sentiment_score", DoubleType()),
            StructField("matched_keywords", StringType()),
        ])
    )
    update_sdf.createOrReplaceTempView("sentiment_update")

    spark.sql("""
    MERGE INTO youtube_mentions_weekly AS target
    USING sentiment_update AS source
    ON target.company = source.company AND target.video_id = source.video_id
    WHEN MATCHED THEN UPDATE SET
        target.sentiment_label = source.sentiment_label,
        target.sentiment_score = source.sentiment_score,
        target.matched_keywords = source.matched_keywords
    """)
    print(f"Scored and merged {len(unscored_pdf)} rows.")
else:
    print("Nothing to score — all rows already have a sentiment_label.")

# COMMAND ----------

display(spark.sql("""
    SELECT sentiment_label, COUNT(*) AS video_count
    FROM youtube_mentions_weekly
    GROUP BY sentiment_label
    ORDER BY video_count DESC
"""))

display(spark.sql("""
    SELECT company, sentiment_label, COUNT(*) AS video_count
    FROM youtube_mentions_weekly
    WHERE sentiment_label IS NOT NULL
    GROUP BY company, sentiment_label
    ORDER BY company, sentiment_label
"""))

display(spark.sql("""
    SELECT title, sentiment_label, matched_keywords
    FROM youtube_mentions_weekly
    WHERE matched_keywords IS NOT NULL AND matched_keywords != ''
    ORDER BY published_at DESC
    LIMIT 20
"""))
