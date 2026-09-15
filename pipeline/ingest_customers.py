"""Pull the customer dimension from the Waseet API into the warehouse.

    python ingest_customers.py

Run this once before the first scan load, because `pipeline.py` checks
customer_id against this table.

Three things about the API, all from Lecture 7:
  * the key goes in the X-API-Key header, not the query string
  * the answer is an envelope; the records are under "customers"
  * the first attempt at every third page comes back 429
"""

import os
import time

import pandas as pd
import requests
from sqlalchemy import create_engine

API_KEY = "waseet-demo-key"
DB_URL = os.environ.get("WASEET_DB_URL",
                        "postgresql+psycopg2://de:de@localhost:5442/waseet")

engine = create_engine(DB_URL)


def get_with_retry(url, headers, params, attempts=4):
    """One GET, with a timeout, and a doubling wait on 429 and 5xx.

    Retry the transient and give up on the permanent. A 401 will not become a
    200 no matter how many times you ask, so we return it and let the caller
    stop rather than retrying a request that can never work.
    """
    wait = 1
    for attempt in range(attempts):
        reply = requests.get(url, headers=headers, params=params, timeout=10)
        if reply.status_code == 429 or reply.status_code >= 500:
            time.sleep(wait)
            wait = wait * 2
            continue
        return reply
    return reply


def fetch_all_customers(base_url):
    """Page until has_more is False. Return a list of dicts.

    The loop is driven by what the API reports in has_more, not by a page count
    worked out here - that would be right only until the day the data grows.
    """
    headers = {"X-API-Key": API_KEY}
    page = 1
    records = []
    while True:
        reply = get_with_retry(base_url + "/customers", headers, {"page": page})
        reply.raise_for_status()
        body = reply.json()
        records.extend(body["customers"])
        if not body["has_more"]:
            break
        page = page + 1
    return records


def load_customers(records):
    """Land them in the customers table.

    Idempotent: the table has a primary key, so a plain append fails on the
    second run. Deleting the rows first and then inserting means running this
    twice leaves the dimension with exactly the same 200 rows.
    """
    df = pd.DataFrame(records)
    with engine.begin() as conn:
        conn.exec_driver_sql("DELETE FROM customers")
        df.to_sql("customers", conn, if_exists="append", index=False)
    return len(df)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, "../api")
    from waseet_api import start_api

    base_url = start_api()
    print("API on", base_url)

    records = fetch_all_customers(base_url)
    print("fetched", len(records), "customers")

    load_customers(records)
    print("loaded")