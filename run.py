import argparse
import json
import logging
import os
import time
from datetime import datetime
from typing import List, Dict, Any

import psycopg2
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from requests import RequestException

from services.repository import Repository

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
load_dotenv()

MAX_RETRIES = 10
BASE_DELAY = 2

def load_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_date(date_str: str) -> datetime.date:
    return datetime.fromisoformat(date_str).date()

def interval_calculation(max_requests_per_day: int = 100) -> float:
    if max_requests_per_day <= 0:
        raise ValueError("max_requests_per_day must be positive")

    requests_needed = max_requests_per_day * 0.8

    allowed_per_hour = requests_needed / 24
    interval_hours = 1 / allowed_per_hour
    interval_minutes = interval_hours * 60

    return interval_minutes


def arg_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spend-file", default="spend.json")
    parser.add_argument("--conv-file", default="conv.json")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()
    return args


def data_processing(start_date: datetime.date, end_date: datetime.date,
                    spend_data: List[Dict[str, Any]], conv_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Combine spend and conversion data, calculate CPA."""

    spend_index = {(row["date"], row["campaign_id"]): row["spend"] for row in spend_data}
    conv_index = {(row["date"], row["campaign_id"]): row["conversions"] for row in conv_data}

    results = []

    keys = set(spend_index.keys()) | set(conv_index.keys())

    for (date, campaign_id) in keys:
        d = parse_date(date)
        if not (start_date <= d <= end_date):
            continue

        spend = spend_index.get((date, campaign_id), 0)
        conversions = conv_index.get((date, campaign_id), 0)

        cpa = None
        if conversions > 0 and spend > 0:
            cpa = round(spend / conversions, 2)

        results.append({
            "date": date,
            "campaign_id": campaign_id,
            "spend": spend,
            "conversions": conversions,
            "cpa": cpa
        })
    return results


def request_api()  -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Request data from API."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # TODO: implement actual API call
            spend_data = [{"date": "2025-06-01", "campaign_id": "TEST", "spend": 100},
                          {"date": "2025-01-02", "campaign_id": "TEST-2", "spend": 30}]
            conv_data = [{"date": "2025-06-06", "campaign_id": "TEST", "conversions":  7},
                         {"date": "2025-01-02", "campaign_id": "TEST-2", "conversions": 14}]
            if spend_data and conv_data:
                return spend_data, conv_data
        except (RequestException, ConnectionError) as e:
            logging.warning(f"Network error on attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                delay = BASE_DELAY * attempt
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error("Max retries reached. Failed to update data.")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            break
    return [], []


def update_data(repo: Repository):
    try:
        spend_data, conv_data = request_api()

        all_dates = [parse_date(d["date"]) for d in spend_data] + [parse_date(d["date"]) for d in conv_data]
        if not all_dates:
            logging.info("No data returned from API")
            return
        start_date = min(all_dates)
        end_date = max(all_dates)

        processed_result = data_processing(start_date, end_date, spend_data, conv_data)
        if processed_result:
            repo.upsert_stats(processed_result)
        logging.info(f"Data updated at {datetime.now()}")
    except Exception as e:
        logging.error(f"Error updating data: {e}")


def main():
    args = arg_parser()

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    spend_data = load_json(args.spend_file)
    conv_data = load_json(args.conv_file)

    results = data_processing(start_date, end_date, spend_data, conv_data)

    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST")
    )

    repo = Repository(conn)
    repo.init_db()
    repo.upsert_stats(results)

    interval_minutes = interval_calculation()
    scheduler = BlockingScheduler()
    scheduler.add_job(update_data, args=(repo,), trigger='interval', minutes=interval_minutes)

    logging.info(f"Scheduler started. Interval: {interval_minutes:.2f} minutes")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped by user.")
    finally:
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
