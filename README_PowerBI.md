# Loading data into Power BI

## 0. Prerequisites (local machine)
```bash
pip install yfinance pandas openpyxl
python fetch_quantum_data.py
```
This produces the following in the `data/` folder:
- `companies.csv`
- `market_size_forecast.csv`
- `segment_share.csv`
- `sentiment.csv`
- `quantum_portfolio.xlsx` (all four tables combined into one workbook)

---

## Option A: Load the Excel file (simplest, recommended)

1. Power BI Desktop → **Get Data** → **Excel workbook**
2. Select `quantum_portfolio.xlsx`
3. In the Navigator, check `Companies`, `MarketSize`, `SegmentShare`, and
   `Sentiment`, then **Load**
4. To refresh: re-run the script to overwrite the file, then hit
   **Refresh** in Power BI

---

## Option B: Load the CSV folder as a whole

1. **Get Data** → **Folder** → point it at the `data` folder
2. All four CSVs get combined into a single query; either filter by
   filename in Power Query to split them back into four queries, or
   simply load each CSV individually via **Text/CSV** — usually easier
   to work with

---

## Option C: Run the Python script directly inside Power BI (for auto-refresh)

If Power BI Desktop can see Python, it can pull fresh data every time you open the file.

1. **File** → **Options and settings** → **Options** → **Python scripting**,
   and point it at your Python install path
2. **Get Data** → **More** → **Python script**
3. Paste in the contents of `fetch_quantum_data.py` (or a trimmed-down
   version) and run it — the `companies`, `market_size`, `segments`, and
   `sentiment` pandas DataFrames created inside the script will each be
   selectable as a table
4. Notes:
   - In Power BI **Desktop** you can refresh manually each time you open
     the file, but scheduled auto-refresh in Power BI **Service** (cloud)
     requires an **on-premises data gateway** with Python installed
   - For personal use / prototyping, Option A (load the Excel file and
     refresh manually) is simpler to set up and more stable

---

## Suggested Power BI visuals

| Data | Sheet/table | Suggested visual |
|---|---|---|
| Market cap comparison | Companies | Horizontal bar chart (log scale) |
| Revenue vs. P/S ratio | Companies | Scatter plot (bubble size = market cap) |
| 1-month / YTD change | Companies | KPI cards + table with conditional formatting |
| Market size forecast | MarketSize | Clustered column chart (by research firm) |
| Segment share | SegmentShare | Horizontal bar chart, sliced by the `dimension` column |
| Sentiment over time | Sentiment | Line chart (date × sentiment_score, one line per ticker) |

> **Note on update frequency:** only `Companies` reflects live data every
> time you re-run `fetch_quantum_data.py`. `MarketSize` and `SegmentShare`
> are static tables built from research reports (see their `source` /
> `source_url` columns) — re-running the script regenerates them with the
> same hardcoded values, it doesn't re-fetch anything. Update those two
> tables by hand in the script whenever the underlying reports change.
> `Sentiment` gets a fresh blank row for today's date each run, but the
> score itself is always manual.

The `Sentiment` sheet is a manually-maintained template. Every time you run
the script it adds a blank row for that day's date, so once you check
Stocktwits/AltIndex and fill in `sentiment_score`, a sentiment trend will
naturally accumulate over time in Power BI.
