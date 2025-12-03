import argparse
import json
import logging
import os
import time
from datetime import datetime
from typing import List, Dict, Any, Tuple

import pandas as pd
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


def data_processing(start_date, end_date, merged_df: pd.DataFrame):
    if merged_df.empty:
        return []

    mask = (merged_df["date"] >= pd.to_datetime(start_date)) & \
           (merged_df["date"] <= pd.to_datetime(end_date))

    df = merged_df.loc[mask].copy()

    df["spend"] = df["spend"].astype(float).fillna(0)
    df["conversions"] = df["conversions"].astype(float).fillna(0)

    df["cpa"] = df.apply(
        lambda r: round(r["spend"] / r["conversions"], 2)
        if r["spend"] > 0 and r["conversions"] > 0
        else None,
        axis=1
    )

    df = df.sort_values(["date", "campaign_id"])
    return df.to_dict(orient="records")


def request_api() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Request data from API."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # TODO: implement actual API call
            spend_data = [{"date": "2025-06-01", "campaign_id": "TEST", "spend": 100},
                          {"date": "2025-01-02", "campaign_id": "TEST-2", "spend": 30}]
            conv_data = [{"date": "2025-06-06", "campaign_id": "TEST", "conversions": 7},
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


def convert_data(spend_data, conv_data) -> pd.DataFrame:
    df_spend = pd.DataFrame(spend_data)
    df_conv = pd.DataFrame(conv_data)

    if df_spend.empty:
        df_spend = pd.DataFrame(columns=['date', 'campaign_id', 'spend'])
    if df_conv.empty:
        df_conv = pd.DataFrame(columns=['date', 'campaign_id', 'conversions'])

    if df_spend.empty and df_conv.empty:
        logging.info("No data returned from API")
        return pd.DataFrame()

    df_merged = pd.merge(df_spend, df_conv, on=['date', 'campaign_id'], how='outer')
    df_merged['date'] = pd.to_datetime(df_merged['date'], format='%Y-%m-%d', errors='coerce')
    df_all = df_merged.dropna(subset=['date', 'campaign_id'])
    return df_all


def update_data(repo: Repository):
    try:
        spend_data, conv_data = request_api()
        df_all = convert_data(spend_data, conv_data)
        if df_all.empty:
            logging.info("No valid dates returned from API")
            return

        start_date = df_all['date'].min().date()
        end_date = df_all['date'].max().date()
        processed_result = data_processing(start_date, end_date, merged_df=df_all)

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
    df_all = convert_data(spend_data, conv_data)

    results = data_processing(start_date, end_date, merged_df=df_all)

    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST")
    )

    repo = Repository(conn)
    repo.init_db()
    repo.upsert_stats(results)

    interval_minutes = 1
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
