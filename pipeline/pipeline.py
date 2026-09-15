"""Waseet Logistics daily scan pipeline.
 
    python pipeline.py --date 2026-05-04
 
extract / check_contract / transform / load, idempotent and logged, with
rejected rows quarantined and a reason on each, and one row written to
load_log for every run - including the runs that fail.
"""
 
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
 
import pandas as pd
from sqlalchemy import create_engine, text
 
DATA_DIR = os.environ.get("WASEET_DATA_DIR", "../data")
DB_URL = os.environ.get("WASEET_DB_URL",
                        "postgresql+psycopg2://de:de@localhost:5442/waseet")
 
# The contract: the columns a scan file must have. The 19th renames weight_kg.
REQUIRED = ["scan_id", "parcel_id", "customer_id", "scanned_at",
            "hub_id", "courier_id", "scan_type", "weight_kg", "service_code"]
 
VALID_TYPES = ["PICKUP", "ARRIVE", "DEPART", "OUT_FOR_DELIVERY",
               "DELIVERED", "FAILED"]
 
LOAD_COLS = ["scan_id", "parcel_id", "customer_id", "scanned_at", "scan_date",
             "hub_id", "courier_id", "scan_type", "weight_kg", "service_code"]
 
os.makedirs("logs", exist_ok=True)
os.makedirs("quarantine", exist_ok=True)
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()])
log = logging.getLogger("waseet")
 
engine = create_engine(DB_URL)
 
 
def extract(scan_date):
    """Read one day's file as strings. dtype=str so pandas does not reinterpret
    values like '12,5' before we have looked at them."""
    path = DATA_DIR + "/scans_" + scan_date + ".csv"
    df = pd.read_csv(path, dtype=str)
    log.info("extract: %d rows from %s", len(df), path)
    return df
 
 
def check_contract(df):
    """Fail the run if the file is not the shape we agreed. Cheap, first,
    before the transform touches anything."""
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError("missing columns: " + str(missing))
    if len(df) == 0:
        raise ValueError("file is empty")
    log.info("contract: ok, %d columns", len(df.columns))
 
 
def parse_timestamps(series):
    """Two formats arrive. Return one datetime column. Roughly 3% of rows use
    DD/MM/YYYY HH:MM instead of ISO; errors='coerce' gives NaT rather than an
    exception, which is what lets fillna merge the two parses."""
    iso = pd.to_datetime(series, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    other = pd.to_datetime(series, format="%d/%m/%Y %H:%M", errors="coerce")
    return iso.fillna(other)
 
 
def transform(raw, scan_date, hubs, couriers, customers, services):
    """Return (good, rejects). Repairs are fixed and kept; rejects are
    quarantined with a reason on every row - a rejected row with no reason
    cannot be argued about."""
    df = raw.copy()
 
    # repairs: fix and keep
    df["scanned_at"] = parse_timestamps(df["scanned_at"])
    df["scan_type"] = df["scan_type"].str.upper()
    df["weight_kg"] = pd.to_numeric(
        df["weight_kg"].str.replace(",", ".", regex=False), errors="coerce")
    for col in ["scan_id", "parcel_id", "customer_id", "hub_id", "courier_id"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
 
    # dedup: the same event sent twice, and the doubled 13th - keep first
    before = len(df)
    df = df.drop_duplicates(subset=["scan_id"], keep="first")
    deduped = before - len(df)
 
    # rejects: quarantine with a reason
    df["reject_reason"] = None
    df.loc[df["scanned_at"].isna(), "reject_reason"] = "unparseable timestamp"
    df.loc[~df["hub_id"].isin(hubs["hub_id"]), "reject_reason"] = "unknown hub"
    df.loc[(df["weight_kg"] > 100) | (df["weight_kg"] <= 0),
           "reject_reason"] = "impossible weight"
    df.loc[~df["scan_type"].isin(VALID_TYPES), "reject_reason"] = "unknown scan_type"
    df.loc[~df["customer_id"].isin(customers["customer_id"]),
           "reject_reason"] = "unknown customer"
    df.loc[~df["service_code"].isin(services["service_code"]),
           "reject_reason"] = "unknown service"
    # a blank courier is a real unassigned scan - not a reject. A courier_id
    # that is present but unknown IS a reject.
    bad_courier = df["courier_id"].notna() & ~df["courier_id"].isin(couriers["courier_id"])
    df.loc[bad_courier, "reject_reason"] = "unknown courier"
 
    good = df[df["reject_reason"].isna()].copy()
    rejects = df[df["reject_reason"].notna()].copy()
 
    if len(rejects) > 0:
        rejects.to_csv("quarantine/rejects_" + scan_date + ".csv", index=False)
        log.warning("quarantined %d rows to quarantine/rejects_%s.csv",
                    len(rejects), scan_date)
 
    good["scan_date"] = good["scanned_at"].dt.date
 
    log.info("transform: %d read = %d good + %d rejected + %d deduped",
             len(raw), len(good), len(rejects), deduped)
    return good, rejects
 
 
def load(good, scan_date):
    """Write one day idempotently. Delete the day first, then insert, so running
    twice for the same date leaves the warehouse exactly as running once did."""
    out = good[LOAD_COLS].copy()
    out["courier_id"] = out["courier_id"].astype("Int64")   # nullable -> NULL
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM parcel_scans WHERE scan_date = :d"),
                     {"d": scan_date})
        out.to_sql("parcel_scans", conn, if_exists="append", index=False)
    log.info("load: %d rows for %s", len(out), scan_date)
    return len(out)
 
 
def write_log(scan_date, started_at, status, read=None, loaded=None,
              rejected=None, message=None):
    """One row per run in load_log - what it read, loaded and rejected, and the
    status. Written on the success path and the failure path alike."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO load_log (scan_date, started_at, finished_at, status,
                                  rows_read, rows_loaded, rows_rejected, message)
            VALUES (:d, :s, :f, :st, :r, :l, :rj, :m)
        """), {"d": scan_date, "s": started_at, "f": datetime.now(timezone.utc),
               "st": status, "r": read, "l": loaded, "rj": rejected, "m": message})
 
 
def main(scan_date):
    started_at = datetime.now(timezone.utc)
    log.info("run starting for %s", scan_date)
    try:
        raw = extract(scan_date)
        check_contract(raw)
        hubs = pd.read_csv(DATA_DIR + "/hubs.csv")
        couriers = pd.read_csv(DATA_DIR + "/couriers.csv")
        services = pd.read_csv(DATA_DIR + "/service_levels.csv")
        customers = pd.read_sql_query("SELECT customer_id FROM customers", engine)
        good, rejects = transform(raw, scan_date, hubs, couriers, customers, services)
        loaded = load(good, scan_date)
        write_log(scan_date, started_at, "success",
                  read=len(raw), loaded=loaded, rejected=len(rejects))
        log.info("run finished for %s", scan_date)
        return 0
    except Exception as problem:
        log.error("run FAILED for %s: %s", scan_date, problem)
        write_log(scan_date, started_at, "failed", message=str(problem))
        return 1
 
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    sys.exit(main(parser.parse_args().date))