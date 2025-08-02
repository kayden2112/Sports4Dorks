import os
import sys 
import logging
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv
# Load environment variables from .env
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(base_dir, '.env'))

API_KEY  = os.getenv("ODDS_API_KEY")
DB_DSN   = os.getenv("DATABASE_URL")
SPORT   = "baseball_mlb"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def fetch_totals():
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds/"
    params = {
        "apiKey": API_KEY,
        "regions": "us",
        "markets": "totals",
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()

def normalize_totals(game):
    game_id = game["id"]
    for book in game["bookmakers"]:
        market = next(m for m in book["markets"] if m["key"] == "totals")
        for o in market["outcomes"]:
            yield (
                game_id,
                o["name"],          # 'Over 8.5' or 'Under 8.5'
                book["key"],        # 'draftkings', 'fanduel', etc.
                o["price"],         # decimal odds
                datetime.utcnow(),
            )

def upsert_totals(rows):
    sql = """
      INSERT INTO game_totals
        (game_id, side, book, odds, timestamp)
      VALUES %s
      ON CONFLICT (game_id, book, side) DO UPDATE
        SET odds      = EXCLUDED.odds,
            timestamp = EXCLUDED.timestamp;
    """
    conn = psycopg2.connect(DB_DSN)
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    try:
        data = fetch_totals()
    except Exception as e:
        logging.error("Failed to fetch totals: %s", e)
        sys.exit(1)

    rows = []
    for game in data:
        rows.extend(list(normalize_totals(game)))

    if not rows:
        logging.warning("No totals data to upsert.")
        sys.exit(0)

    upsert_totals(rows)
    logging.info("Upserted %d total-run rows", len(rows))