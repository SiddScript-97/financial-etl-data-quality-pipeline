"""
Script to generate a realistic sample startup_funding.csv dataset.
Run this once to create the raw data for the ETL pipeline.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
(BASE_DIR / "data").mkdir(exist_ok=True)

random.seed(42)

COMPANIES = [
    ("TechNova", "Technology"), ("GreenLeaf Energy", "Energy"), ("HealthAI", "Healthcare"),
    ("FinEdge", "Finance"), ("EduSpark", "Education"), ("AgriTech India", "Agriculture"),
    ("RetailBot", "Retail"), ("LogiFlow", "Logistics"), ("CyberShield", "Cybersecurity"),
    ("BioGenix", "Biotechnology"), ("SolarGrid", "Energy"), ("MedAssist", "Healthcare"),
    ("CryptoVault", "Finance"), ("SkillUp", "Education"), ("FarmLink", "Agriculture"),
    ("ShopEase", "Retail"), ("CargoNet", "Logistics"), ("DataGuard", "Cybersecurity"),
    ("NanoMed", "Biotechnology"), ("WindCore", "Energy"), ("TeleMed", "Healthcare"),
    ("PaySwift", "Finance"), ("LearnAI", "Education"), ("SoilSense", "Agriculture"),
    ("DealHub", "Retail"), ("FreightX", "Logistics"), ("SecureNet", "Cybersecurity"),
    ("GeneTech", "Biotechnology"), ("CleanFuel", "Energy"), ("DiagnoAI", "Healthcare"),
    ("WealthBot", "Finance"), ("CodeCamp", "Education"), ("HarvestPro", "Agriculture"),
    ("QuickMart", "Retail"), ("TrackIt", "Logistics"), ("FireWall Pro", "Cybersecurity"),
    ("CellCure", "Biotechnology"), ("HydroGen", "Energy"), ("PatientIQ", "Healthcare"),
    ("InvestAI", "Finance"), ("TutorBot", "Education"), ("IrrigaTech", "Agriculture"),
    ("BrandBoost", "Retail"), ("PortEx", "Logistics"), ("VaultSec", "Cybersecurity"),
    ("PharmaLink", "Biotechnology"), ("ThermoGen", "Energy"), ("NurseAI", "Healthcare"),
    ("LoanFlow", "Finance"), ("VidLearn", "Education"),
]

INVESTORS = [
    "Sequoia Capital", "Accel Partners", "Tiger Global", "SoftBank Vision Fund",
    "Nexus Venture Partners", "Kalaari Capital", "Matrix Partners", "Blume Ventures",
    "HDFC Capital", "Tata Capital", "Lightspeed Venture", "Peak XV Partners",
]

CITIES = ["Bangalore", "Mumbai", "Delhi", "Hyderabad", "Pune", "Chennai",
          "Kolkata", "Ahmedabad", "Noida", "Gurugram"]

ROUNDS = ["Seed", "Series A", "Series B", "Series C", "Pre-Series A", "Bridge"]

def random_date(start_year=2018, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")

def random_funding(round_type):
    ranges = {
        "Seed": (50000, 2000000),
        "Pre-Series A": (500000, 5000000),
        "Series A": (2000000, 20000000),
        "Bridge": (1000000, 8000000),
        "Series B": (15000000, 80000000),
        "Series C": (50000000, 300000000),
    }
    lo, hi = ranges.get(round_type, (100000, 10000000))
    return round(random.uniform(lo, hi), 2)

rows = []
for i, (company, sector) in enumerate(COMPANIES):
    for _ in range(random.randint(1, 3)):
        round_type = random.choice(ROUNDS)
        funding = random_funding(round_type)
        # Introduce messy formats deliberately
        fmt = random.choice(["plain", "dollar", "comma_dollar", "indian"])
        if fmt == "dollar":
            funding_str = f"${funding:,.2f}"
        elif fmt == "comma_dollar":
            funding_str = f"${int(funding):,}"
        elif fmt == "indian":
            funding_str = f"₹{funding:,.2f}"
        else:
            funding_str = str(funding)

        rows.append({
            "company_name": company,
            "sector": sector,
            "city": random.choice(CITIES),
            "funding_amount": funding_str,
            "round": round_type,
            "investor": random.choice(INVESTORS),
            "date": random_date(),
            "country": "India",
        })

# Inject intentional quality issues
# 1. Nulls
for _ in range(8):
    r = random.choice(rows)
    r["funding_amount"] = ""

# 2. Duplicates
rows.extend(random.sample(rows, 5))

# 3. Negative values
for _ in range(3):
    random.choice(rows)["funding_amount"] = str(random.uniform(-5000, -100))

# 4. Zero values
for _ in range(2):
    random.choice(rows)["funding_amount"] = "0"

random.shuffle(rows)

with open(BASE_DIR / "data" / "startup_funding.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} rows → data/startup_funding.csv")
