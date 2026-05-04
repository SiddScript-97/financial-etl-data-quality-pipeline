"""
Financial Data ETL Pipeline with Data Quality Validation
=========================================================
Modules:
  1. Extractor   — load raw CSV (or scrape web data)
  2. Transformer — clean, normalise, type-cast
  3. Validator   — run quality checks, emit a report
  4. Loader      — persist clean records to SQLite
  5. Analyser    — run canned SQL queries and show insights

Usage:
    python etl_pipeline.py

Outputs:
    output/financial_data.db   — SQLite database (companies table)
    output/quality_report.json — Data-quality audit report
    logs/pipeline.log          — Full run log
"""

import os
import re
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR    = BASE_DIR / "logs"

for d in (OUTPUT_DIR, LOG_DIR):
    d.mkdir(exist_ok=True)

DB_PATH      = OUTPUT_DIR / "financial_data.db"
REPORT_PATH  = OUTPUT_DIR / "quality_report.json"
CSV_PATH     = DATA_DIR   / "startup_funding.csv"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("etl")


# ===========================================================================
# 1. EXTRACTOR
# ===========================================================================

class Extractor:
    """Load raw financial data from a CSV file."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path

    def extract(self) -> pd.DataFrame:
        log.info("─" * 60)
        log.info("STEP 1 — EXTRACTION")
        log.info(f"  Source : {self.csv_path}")

        if not self.csv_path.exists():
            raise FileNotFoundError(
                f"Source file not found: {self.csv_path}\n"
                "Run  python generate_data.py  first."
            )

        df = pd.read_csv(self.csv_path, dtype=str)  # load everything as str initially
        log.info(f"  Loaded : {len(df):,} rows  ×  {len(df.columns)} columns")
        log.info(f"  Columns: {list(df.columns)}")
        return df


# ===========================================================================
# 2. TRANSFORMER
# ===========================================================================

class Transformer:
    """Clean and normalise raw financial records."""

    # Regex strips $, ₹, commas, and surrounding whitespace
    _CURRENCY_RE = re.compile(r"[\$₹,\s]")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        log.info("─" * 60)
        log.info("STEP 2 — TRANSFORMATION")
        raw_count = len(df)

        df = df.copy()

        # ── 2a. Strip whitespace from all string columns ──────────────────
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

        # ── 2b. Replace blank strings with NaN ────────────────────────────
        df.replace("", np.nan, inplace=True)

        # ── 2c. Normalise funding_amount → float ──────────────────────────
        def parse_funding(val):
            if pd.isna(val):
                return np.nan
            cleaned = self._CURRENCY_RE.sub("", str(val))
            try:
                return float(cleaned)
            except ValueError:
                return np.nan

        df["funding_amount"] = df["funding_amount"].apply(parse_funding)

        # ── 2d. Normalise dates ────────────────────────────────────────────
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["year"] = df["date"].dt.year
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        # ── 2e. Title-case text fields ────────────────────────────────────
        for col in ("company_name", "sector", "city", "country", "round"):
            if col in df.columns:
                df[col] = df[col].str.title()

        # ── 2f. Remove exact duplicate rows ───────────────────────────────
        before_dedup = len(df)
        df.drop_duplicates(inplace=True)
        after_dedup = len(df)
        log.info(f"  Duplicates removed : {before_dedup - after_dedup}")

        # ── 2g. Drop rows where funding is null ───────────────────────────
        before_null_drop = len(df)
        df.dropna(subset=["funding_amount"], inplace=True)
        log.info(f"  Null funding rows  : {before_null_drop - len(df)}")

        # ── 2h. Remove invalid funding (≤ 0) ─────────────────────────────
        before_invalid = len(df)
        df = df[df["funding_amount"] > 0]
        log.info(f"  Invalid ≤0 removed : {before_invalid - len(df)}")

        # ── 2i. Reset index ───────────────────────────────────────────────
        df.reset_index(drop=True, inplace=True)

        log.info(f"  Rows after clean   : {len(df):,}  (started: {raw_count:,})")
        return df


# ===========================================================================
# 3. VALIDATOR
# ===========================================================================

class Validator:
    """Run data-quality checks and produce an audit report."""

    def validate(self, df: pd.DataFrame) -> dict:
        log.info("─" * 60)
        log.info("STEP 3 — DATA QUALITY VALIDATION")

        report = {
            "run_timestamp"   : datetime.now().isoformat(),
            "total_records"   : len(df),
            "missing_values"  : df.isnull().sum().to_dict(),
            "duplicates"      : int(df.duplicated().sum()),
            "invalid_funding" : int((df["funding_amount"] <= 0).sum()),
            "negative_funding": int((df["funding_amount"] < 0).sum()),
            "zero_funding"    : int((df["funding_amount"] == 0).sum()),
            "date_nulls"      : int(df["date"].isna().sum()),
            "schema_ok"       : self._check_schema(df),
            "funding_stats"   : {
                "min"   : round(float(df["funding_amount"].min()), 2),
                "max"   : round(float(df["funding_amount"].max()), 2),
                "mean"  : round(float(df["funding_amount"].mean()), 2),
                "median": round(float(df["funding_amount"].median()), 2),
            },
            "records_per_sector": df["sector"].value_counts().to_dict(),
            "records_per_round" : df["round"].value_counts().to_dict(),
        }

        # Summary flag
        issues = (
            report["duplicates"] +
            report["invalid_funding"] +
            report["date_nulls"] +
            sum(v for v in report["missing_values"].values() if v)
        )
        report["quality_score"] = max(0, 100 - issues)
        report["status"] = "PASS" if issues == 0 else "WARN"

        self._log_report(report)
        return report

    def _check_schema(self, df: pd.DataFrame) -> bool:
        required = {"company_name", "sector", "funding_amount", "date", "round"}
        return required.issubset(set(df.columns))

    def _log_report(self, r: dict):
        log.info(f"  Records           : {r['total_records']:,}")
        log.info(f"  Duplicates        : {r['duplicates']}")
        log.info(f"  Invalid funding   : {r['invalid_funding']}")
        log.info(f"  Schema OK         : {r['schema_ok']}")
        log.info(f"  Quality score     : {r['quality_score']}/100  [{r['status']}]")
        log.info(f"  Funding range     : ${r['funding_stats']['min']:,.0f}"
                 f" – ${r['funding_stats']['max']:,.0f}")


# ===========================================================================
# 4. LOADER
# ===========================================================================

class Loader:
    """Persist clean data to SQLite."""

    def __init__(self, db_path: Path):
        self.db_path = db_path

    def load(self, df: pd.DataFrame) -> None:
        log.info("─" * 60)
        log.info("STEP 4 — LOADING")
        log.info(f"  Target : {self.db_path}")

        conn = sqlite3.connect(self.db_path)
        try:
            self._create_schema(conn)
            df.to_sql("companies", conn, if_exists="replace", index=False)
            conn.execute(
                "INSERT OR REPLACE INTO pipeline_runs VALUES (?,?,?)",
                (None, datetime.now().isoformat(), len(df))
            )
            conn.commit()
            log.info(f"  Written: {len(df):,} rows → 'companies' table")
        finally:
            conn.close()

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS companies (
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

            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at     TEXT,
                rows_loaded INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_sector ON companies(sector);
            CREATE INDEX IF NOT EXISTS idx_year   ON companies(year);
            CREATE INDEX IF NOT EXISTS idx_round  ON companies(round);
        """)


# ===========================================================================
# 5. ANALYSER
# ===========================================================================

class Analyser:
    """Run insight queries against the loaded database."""

    QUERIES = {
        "sector_funding": """
            SELECT   sector,
                     COUNT(*)                               AS deals,
                     ROUND(SUM(funding_amount), 2)          AS total_funding,
                     ROUND(AVG(funding_amount), 2)          AS avg_funding
            FROM     companies
            GROUP BY sector
            ORDER BY total_funding DESC;
        """,
        "top_companies": """
            SELECT   company_name,
                     sector,
                     ROUND(SUM(funding_amount), 2) AS total_raised
            FROM     companies
            GROUP BY company_name, sector
            ORDER BY total_raised DESC
            LIMIT    15;
        """,
        "yearly_trend": """
            SELECT   year,
                     COUNT(*)                               AS deals,
                     ROUND(SUM(funding_amount) / 1e6, 2)   AS total_usd_m
            FROM     companies
            WHERE    year IS NOT NULL
            GROUP BY year
            ORDER BY year;
        """,
        "round_distribution": """
            SELECT   round,
                     COUNT(*)                               AS deals,
                     ROUND(AVG(funding_amount), 2)          AS avg_funding
            FROM     companies
            GROUP BY round
            ORDER BY avg_funding DESC;
        """,
        "top_investors": """
            SELECT   investor,
                     COUNT(*)                               AS deals,
                     ROUND(SUM(funding_amount), 2)          AS total_deployed
            FROM     companies
            WHERE    investor IS NOT NULL
            GROUP BY investor
            ORDER BY deals DESC
            LIMIT    10;
        """,
        "city_hotspots": """
            SELECT   city,
                     COUNT(*) AS deals
            FROM     companies
            WHERE    city IS NOT NULL
            GROUP BY city
            ORDER BY deals DESC
            LIMIT    10;
        """,
    }

    def __init__(self, db_path: Path):
        self.db_path = db_path

    def analyse(self) -> dict:
        log.info("─" * 60)
        log.info("STEP 5 — ANALYSIS & INSIGHTS")
        results = {}
        conn = sqlite3.connect(self.db_path)
        try:
            for name, sql in self.QUERIES.items():
                df = pd.read_sql_query(sql, conn)
                results[name] = df.to_dict(orient="records")
                log.info(f"  {name:<25} → {len(df)} rows")
        finally:
            conn.close()
        return results

    def print_insights(self, results: dict) -> None:
        divider = "=" * 60

        print(f"\n{divider}")
        print("  SECTOR FUNDING SUMMARY")
        print(divider)
        for r in results.get("sector_funding", []):
            print(f"  {r['sector']:<20} {r['deals']:>4} deals   "
                  f"${r['total_funding']:>15,.0f}  total")

        print(f"\n{divider}")
        print("  TOP 10 COMPANIES BY TOTAL RAISED")
        print(divider)
        for i, r in enumerate(results.get("top_companies", [])[:10], 1):
            print(f"  {i:>2}. {r['company_name']:<22} ({r['sector']:<15}) "
                  f"${r['total_raised']:>12,.0f}")

        print(f"\n{divider}")
        print("  YEARLY INVESTMENT TREND")
        print(divider)
        for r in results.get("yearly_trend", []):
            bar = "█" * max(1, int(r["total_usd_m"] / 5))
            print(f"  {r['year']}  {bar:<40}  ${r['total_usd_m']:.1f}M  ({r['deals']} deals)")

        print(f"\n{divider}")
        print("  FUNDING ROUND DISTRIBUTION")
        print(divider)
        for r in results.get("round_distribution", []):
            print(f"  {r['round']:<15} {r['deals']:>4} deals   "
                  f"avg ${r['avg_funding']:>12,.0f}")

        print(f"\n{divider}")
        print("  TOP INVESTORS BY DEAL COUNT")
        print(divider)
        for r in results.get("top_investors", []):
            print(f"  {r['investor']:<30} {r['deals']:>4} deals")

        print(f"\n{divider}")
        print("  STARTUP CITY HOTSPOTS")
        print(divider)
        for r in results.get("city_hotspots", []):
            print(f"  {r['city']:<20} {r['deals']:>4} deals")
        print()


# ===========================================================================
# PIPELINE ORCHESTRATOR
# ===========================================================================

def run_pipeline():
    log.info("=" * 60)
    log.info("  FINANCIAL DATA ETL PIPELINE — START")
    log.info("=" * 60)
    start = datetime.now()

    try:
        # 1. Extract
        raw_df = Extractor(CSV_PATH).extract()

        # 2. Transform
        clean_df = Transformer().transform(raw_df)

        # 3. Validate
        validator = Validator()
        report    = validator.validate(clean_df)

        # 4. Load
        Loader(DB_PATH).load(clean_df)

        # 5. Analyse
        analyser = Analyser(DB_PATH)
        results  = analyser.analyse()
        analyser.print_insights(results)

        # Persist quality report
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
        log.info(f"  Quality report saved → {REPORT_PATH}")

        elapsed = (datetime.now() - start).total_seconds()
        log.info("─" * 60)
        log.info(f"  Pipeline COMPLETE in {elapsed:.2f}s")
        log.info("=" * 60)

        return {"status": "success", "report": report, "results": results}

    except Exception as exc:
        log.exception(f"Pipeline FAILED: {exc}")
        return {"status": "error", "message": str(exc)}


if __name__ == "__main__":
    run_pipeline()
