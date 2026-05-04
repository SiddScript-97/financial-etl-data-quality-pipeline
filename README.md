# 📊 Financial Data ETL Pipeline with Data Quality Validation

A production-style ETL pipeline that processes startup funding data — extracting
from CSV, cleaning/normalising, validating quality, loading to SQLite, and
running analytical queries.

---

## 🗂️ Project Structure

```
financial_etl_pipeline/
├── generate_data.py      # One-time script: generate realistic sample CSV
├── etl_pipeline.py       # Main ETL pipeline (5 stages)
├── requirements.txt      # Python dependencies
├── data/
│   └── startup_funding.csv   # Raw input (created by generate_data.py)
├── output/
│   ├── financial_data.db     # SQLite database (created by pipeline)
│   └── quality_report.json   # Data quality audit (created by pipeline)
└── logs/
    └── pipeline.log          # Full run log (created by pipeline)
```

---

## ⚡ Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate sample data
python generate_data.py

# 3. Run the full pipeline
python etl_pipeline.py
```

---

## 🏗️ Architecture

```
Raw CSV
  │
  ▼
[Extractor]   → Loads CSV with Pandas (all columns as strings)
  │
  ▼
[Transformer] → Strips whitespace · Parses $₹ currencies · Normalises dates
              → Removes duplicates · Drops null/zero/negative funding rows
  │
  ▼
[Validator]   → Missing values · Duplicates · Invalid funding
              → Schema check · Quality score (0–100) · Audit JSON
  │
  ▼
[Loader]      → SQLite: companies table + indexes + pipeline_runs log
  │
  ▼
[Analyser]    → 6 insight queries: sector trends, top companies,
                yearly growth, round distribution, investors, city hotspots
```

---

## 🧹 Transformation Steps

| Issue | How it's handled |
|---|---|
| `$1,234,567` / `₹9,00,000` | Regex strips `$  ₹  ,` → `float` |
| Empty strings | Replaced with `NaN` |
| Inconsistent dates | `pd.to_datetime(errors='coerce')` → `YYYY-MM-DD` |
| Duplicate rows | `drop_duplicates()` |
| Null funding | `dropna(subset=['funding_amount'])` |
| Negative / zero funding | Filtered out with `df[df['funding_amount'] > 0]` |
| Inconsistent casing | `str.title()` on text columns |

---

## ✅ Data Quality Checks

The `Validator` class runs these checks and emits `output/quality_report.json`:

- **Missing values** — per-column null counts
- **Duplicate rows** — exact row duplicates
- **Invalid funding** — negative or zero values
- **Date nulls** — unparseable date strings
- **Schema check** — required columns present
- **Quality score** — `max(0, 100 − total_issues)`
- **Funding statistics** — min / max / mean / median

---

## 🗄️ Database Schema (SQLite)

```sql
CREATE TABLE companies (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name   TEXT    NOT NULL,
    sector         TEXT,
    city           TEXT,
    funding_amount REAL    NOT NULL,
    round          TEXT,
    investor       TEXT,
    date           TEXT,
    year           INTEGER,
    country        TEXT
);
```

Indexes on `sector`, `year`, `round` for fast analytical queries.

---

## 📈 Sample Insight Queries

```sql
-- Sector-wise total investment
SELECT sector, COUNT(*) AS deals, ROUND(SUM(funding_amount),2) AS total_funding
FROM companies GROUP BY sector ORDER BY total_funding DESC;

-- Top funded companies
SELECT company_name, sector, ROUND(SUM(funding_amount),2) AS total_raised
FROM companies GROUP BY company_name ORDER BY total_raised DESC LIMIT 15;

-- Yearly trend (in $M)
SELECT year, COUNT(*) AS deals, ROUND(SUM(funding_amount)/1e6,2) AS total_usd_m
FROM companies WHERE year IS NOT NULL GROUP BY year ORDER BY year;
```

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Data ingestion | `pandas.read_csv` |
| Transformation | Pandas + NumPy + `re` |
| Quality checks | Custom `Validator` class |
| Storage | `sqlite3` (standard library) |
| Querying | `pandas.read_sql_query` |

---

## 🚀 Future Enhancements

- **Orchestration** — Apache Airflow DAG for scheduled runs
- **Dashboard** — Streamlit app with charts (sector pie, yearly bar, heatmap)
- **LLM extraction** — Use Claude API to parse unstructured funding news
- **Cloud** — AWS S3 source → RDS target via Lambda
- **Alerting** — Slack/email notification when quality score drops below threshold
- **Web scraping** — BeautifulSoup / Scrapy for live Crunchbase-style data

---

## 📎 Outputs

| File | Description |
|---|---|
| `output/financial_data.db` | Clean, queryable SQLite database |
| `output/quality_report.json` | Machine-readable audit with quality score |
| `logs/pipeline.log` | Timestamped run log for every pipeline execution |
