import logging
import requests
import psycopg2
import json
from datetime import datetime, timezone
import time
import os

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)

DB_URL = os.getenv("DATABASE_URL")
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

def insert_category(category: str):
    url = "https://api.adzuna.com/v1/api/jobs/us/top_companies"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "category": category
    }

    try:
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

        logging.info(f"✅ Inserted: {category}")
        time.sleep(1)

    except Exception as e:
        logging.error(f"❌ Error for {category}: {e}")
