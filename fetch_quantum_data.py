"""
fetch_quantum_data.py
======================
Fetches data for a quantum-computing company portfolio and exports it in
formats Power BI can read directly (CSV / a single multi-sheet Excel file).

Output (./data/):
  - companies.csv            Price, market cap, revenue, P/S ratio, etc. (live via yfinance)
  - market_size_forecast.csv Market size forecasts by research firm (static, manually maintained)
  - segment_share.csv        Segment share by offering/deployment/application (static)
  - sentiment.csv            Social sentiment (manually-maintained template)
  - quantum_portfolio.xlsx   All four tables combined as sheets in one workbook

Usage:
  pip install yfinance pandas openpyxl
  python fetch_quantum_data.py

See README_PowerBI.md for how to load this into Power BI.
"""

import datetime as dt
import os
import sys
import time
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    sys.exit("yfinance not found. Run `pip install yfinance` first.")

OUT_DIR = "data"
os.makedirs(OUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# 1. Static master data (things yfinance doesn't know: category, technology
#    approach, etc.). Edit this list to add/remove companies. A ticker of
#    None means the company is not yet public / not covered by yfinance, so
#    price data is skipped for it.
# ---------------------------------------------------------------------------
COMPANY_MASTER = [
    {"name": "IonQ",                     "ticker": "IONQ", "category": "Pure-play quantum", "approach": "Trapped-ion"},
    {"name": "Rigetti Computing",        "ticker": "RGTI", "category": "Pure-play quantum", "approach": "Superconducting"},
    {"name": "D-Wave Quantum",           "ticker": "QBTS", "category": "Pure-play quantum", "approach": "Annealing + gate-model"},
    {"name": "Quantum Computing Inc.",   "ticker": "QUBT", "category": "Pure-play quantum", "approach": "Photonics"},
    {"name": "Quantinuum",               "ticker": None,   "category": "Pure-play quantum", "approach": "Trapped-ion"},
    {"name": "IQM Quantum Computers",    "ticker": None,   "category": "Pure-play quantum", "approach": "Superconducting"},
    {"name": "IBM",                      "ticker": "IBM",  "category": "Big tech",          "approach": "Superconducting"},
    {"name": "Alphabet (Google)",        "ticker": "GOOG", "category": "Big tech",          "approach": "Superconducting"},
    {"name": "Microsoft",                "ticker": "MSFT", "category": "Big tech",          "approach": "Topological"},
    {"name": "NVIDIA",                   "ticker": "NVDA", "category": "Big tech",          "approach": "CUDA-Q (hybrid platform)"},
    {"name": "SEALSQ",                   "ticker": "LAES", "category": "Post-quantum security", "approach": "Quantum-resistant semiconductors / PKI"},
    {"name": "BTQ Technologies",         "ticker": "BTQ",  "category": "Post-quantum security", "approach": "Post-quantum cryptography"},
]


def fetch_company_row(master_row: dict) -> dict:
    """Fetch one company's live data from yfinance and return it as a dict.
    Fills fields with None if the fetch fails."""
    row = {
        "name": master_row["name"],
        "ticker": master_row["ticker"],
        "category": master_row["category"],
        "approach": master_row["approach"],
        "price_usd": None,
        "market_cap_b": None,
        "quarterly_revenue_m": None,
        "ps_ratio": None,
        "change_1mo_pct": None,
        "change_ytd_pct": None,
        "as_of": dt.date.today().isoformat(),
    }

    if not master_row["ticker"]:
        row["note"] = "Not yet public / not covered by yfinance — update manually"
        return row

    try:
        t = yf.Ticker(master_row["ticker"])
        info = t.info or {}

        row["price_usd"] = info.get("currentPrice") or info.get("regularMarketPrice")
        mcap = info.get("marketCap")
        row["market_cap_b"] = round(mcap / 1e9, 2) if mcap else None

        # P/S ratio: use yfinance's own field if present, otherwise
        # approximate as market_cap / (quarterly_revenue * 4).
        ps = info.get("priceToSalesTrailing12Months")

        # Most recent quarterly revenue
        rev_m = None
        try:
            qfin = t.quarterly_income_stmt  # newer yfinance API
            if qfin is not None and "Total Revenue" in qfin.index:
                rev_m = round(qfin.loc["Total Revenue"].iloc[0] / 1e6, 2)
        except Exception:
            pass
        if rev_m is None:
            try:
                qfin = t.quarterly_financials
                if qfin is not None and "Total Revenue" in qfin.index:
                    rev_m = round(qfin.loc["Total Revenue"].iloc[0] / 1e6, 2)
            except Exception:
                pass
        row["quarterly_revenue_m"] = rev_m

        if ps:
            row["ps_ratio"] = round(ps, 1)
        elif mcap and rev_m:
            row["ps_ratio"] = round(mcap / (rev_m * 1e6 * 4), 1)

        # 1-month and year-to-date price change (close-to-close)
        hist_1mo = t.history(period="1mo")
        if not hist_1mo.empty:
            first, last = hist_1mo["Close"].iloc[0], hist_1mo["Close"].iloc[-1]
            row["change_1mo_pct"] = round((last / first - 1) * 100, 1)

        hist_ytd = t.history(period="ytd")
        if not hist_ytd.empty:
            first, last = hist_ytd["Close"].iloc[0], hist_ytd["Close"].iloc[-1]
            row["change_ytd_pct"] = round((last / first - 1) * 100, 1)

        row["note"] = ""

    except Exception as e:
        row["note"] = f"Fetch error: {e}"

    return row


def build_companies_table() -> pd.DataFrame:
    rows = []
    for m in COMPANY_MASTER:
        rows.append(fetch_company_row(m))
        time.sleep(0.5)  # basic rate-limit courtesy
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 2. Market size forecast (static, manually maintained)
#    Sources: Fortune Business Insights / Grand View Research / Precedence
#    Research / Market.us / SNS Insider (reports published Jan-Jul 2026)
# ---------------------------------------------------------------------------
def build_market_size_table() -> pd.DataFrame:
    return pd.DataFrame([
        {"source": "Fortune Business Insights", "source_url": "https://www.fortunebusinessinsights.com/quantum-computing-market-104855", "base_year": 2025, "base_value_b": 1.39, "final_year": 2034, "final_value_b": 17.89, "cagr_pct": 33.0},
        {"source": "Grand View Research",        "source_url": "https://www.grandviewresearch.com/industry-analysis/quantum-computing-market", "base_year": 2025, "base_value_b": 1.60, "final_year": 2033, "final_value_b": 8.00,  "cagr_pct": 22.3},
        {"source": "Precedence Research",        "source_url": "https://www.precedenceresearch.com/quantum-computing-market", "base_year": 2025, "base_value_b": 1.44, "final_year": 2035, "final_value_b": 19.44, "cagr_pct": 29.73},
        {"source": "Market.us",                  "source_url": "https://market.us/report/quantum-computing-market/", "base_year": 2025, "base_value_b": 2.20, "final_year": 2035, "final_value_b": 50.40, "cagr_pct": 37.0},
        {"source": "SNS Insider",                "source_url": "https://www.snsinsider.com/reports/quantum-computing-market-2740", "base_year": 2025, "base_value_b": 1.47, "final_year": 2035, "final_value_b": 18.91, "cagr_pct": 29.1},
    ])


# ---------------------------------------------------------------------------
# 3. Segment share (static, source: Grand View Research 2025)
#    The `dimension` column separates the classification axes — only compare
#    rows within the same dimension.
# ---------------------------------------------------------------------------
def build_segment_table() -> pd.DataFrame:
    src = "Grand View Research"
    src_url = "https://www.grandviewresearch.com/industry-analysis/quantum-computing-market"
    return pd.DataFrame([
        {"dimension": "Offering",   "segment": "Systems",            "share_pct": 63.5, "source": src, "source_url": src_url},
        {"dimension": "Offering",   "segment": "Services",           "share_pct": 36.5, "source": src, "source_url": src_url},
        {"dimension": "Deployment", "segment": "On-premise",         "share_pct": 48.4, "source": src, "source_url": src_url},
        {"dimension": "Deployment", "segment": "Cloud",              "share_pct": 51.6, "source": src, "source_url": src_url},
        {"dimension": "Application","segment": "Optimization",       "share_pct": 29.3, "source": src, "source_url": src_url},
        {"dimension": "Industry",   "segment": "BFSI (finance/insurance)", "share_pct": 21.7, "source": src, "source_url": src_url},
    ])


# ---------------------------------------------------------------------------
# 4. Social sentiment (manually-maintained template)
#    No free real-time API exists for this, so it's a placeholder: fill in
#    scores by checking Stocktwits / Reddit / AltIndex etc. periodically.
#    Columns are pre-defined so that, once you accumulate rows over time in
#    Power BI, you can chart a sentiment trend by date.
# ---------------------------------------------------------------------------
def build_sentiment_table() -> pd.DataFrame:
    today = dt.date.today().isoformat()
    return pd.DataFrame([
        {"date": today, "ticker": "IONQ", "sentiment_score": None, "sentiment_label": "", "mention_volume": "", "source": "Manual — check Stocktwits / AltIndex etc. and fill in"},
        {"date": today, "ticker": "RGTI", "sentiment_score": None, "sentiment_label": "", "mention_volume": "", "source": "Manual — check Stocktwits / AltIndex etc. and fill in"},
        {"date": today, "ticker": "QBTS", "sentiment_score": None, "sentiment_label": "", "mention_volume": "", "source": "Manual — check Stocktwits / AltIndex etc. and fill in"},
        {"date": today, "ticker": "QUBT", "sentiment_score": None, "sentiment_label": "", "mention_volume": "", "source": "Manual — check Stocktwits / AltIndex etc. and fill in"},
    ])


def main():
    print("1/4 Fetching company data (price / financials) ...")
    companies = build_companies_table()

    print("2/4 Building market size forecast table ...")
    market_size = build_market_size_table()

    print("3/4 Building segment share table ...")
    segments = build_segment_table()

    print("4/4 Building sentiment template ...")
    sentiment = build_sentiment_table()

    companies.to_csv(f"{OUT_DIR}/companies.csv", index=False, encoding="utf-8-sig")
    market_size.to_csv(f"{OUT_DIR}/market_size_forecast.csv", index=False, encoding="utf-8-sig")
    segments.to_csv(f"{OUT_DIR}/segment_share.csv", index=False, encoding="utf-8-sig")
    sentiment.to_csv(f"{OUT_DIR}/sentiment.csv", index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(f"{OUT_DIR}/quantum_portfolio.xlsx", engine="openpyxl") as writer:
        companies.to_excel(writer, sheet_name="Companies", index=False)
        market_size.to_excel(writer, sheet_name="MarketSize", index=False)
        segments.to_excel(writer, sheet_name="SegmentShare", index=False)
        sentiment.to_excel(writer, sheet_name="Sentiment", index=False)

    print(f"\nDone. Output written to ./{OUT_DIR}/")
    print(companies[["name", "ticker", "market_cap_b", "quarterly_revenue_m", "ps_ratio", "change_1mo_pct"]])


if __name__ == "__main__":
    main()
