import logging
import requests
import psycopg2
import json
from datetime import datetime, timezone
import time
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)

# Get environment variables
DB_URL = os.getenv("DATABASE_URL")
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

# All Adzuna categories
adzuna_categories = [
    "accounting-finance-jobs",
    "admin-jobs",
    "consultancy-jobs",
    "customer-services-jobs",
    "energy-oil-gas-jobs",
    "engineering-jobs",
    "graduate-jobs",
    "healthcare-nursing-jobs",
    "hospitality-catering-jobs",
    "hr-jobs",
    "it-jobs",
    "legal-jobs",
    "logistics-warehouse-jobs",
    "maintenance-jobs",
    "manufacturing-jobs",
    "other-general-jobs",
    "part-time-jobs",
    "property-jobs",
    "retail-jobs",
    "sales-jobs",
    "scientific-qa-jobs",
    "teaching-jobs",
    "trade-construction-jobs",
    "travel-jobs"
]

def insert_category(category: str):
    url = "https://api.adzuna.com/v1/api/jobs/us/top_companies"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "category": category
    }

    try:
        # API request
        response = requests.get(url, params=params)
        response.raise_for_status()
        raw_json = response.json()

        if "leaderboard" not in raw_json or not raw_json["leaderboard"]:
            logging.warning(f"No leaderboard returned for {category}")
            return

        logging.info(f"Inserting {category} with {len(raw_json['leaderboard'])} companies...")

        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO final.adzuna_top_companies_raw 
            (raw_json, category_tag, scraped_at, normalized)
            VALUES (%s, %s, %s, %s);
        """, (
            json.dumps(raw_json),
            category,
            datetime.now(timezone.utc),
            False
        ))
        conn.commit()
        cur.close()
        conn.close()

        logging.info(f"Inserted: {category}")
        time.sleep(1)

    except Exception as e:
        logging.error(f"Error for {category}: {e}")

if __name__ == "__main__":
    for category in adzuna_categories:
        insert_category(category)
