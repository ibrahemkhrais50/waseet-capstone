"""The Waseet quality suite.
 
Rules are data. The suite below is a list a warehouse manager could review
without reading Python, which is the whole reason it is not a set of .loc masks
inside transform().
 
    python quality.py --date 2026-05-04
 
Exit code is 0 when every rule passes and 1 when any rule fails, so the suite
can be a task in the DAG - a quality script that always exits 0 cannot.
"""
 
import argparse
import json
import os
import sys
 
import pandas as pd
from sqlalchemy import create_engine
 
DATA_DIR = os.environ.get("WASEET_DATA_DIR", "../data")
DB_URL = os.environ.get("WASEET_DB_URL",
                        "postgresql+psycopg2://de:de@localhost:5442/waseet")
 
VALID_TYPES = ["PICKUP", "ARRIVE", "DEPART", "OUT_FOR_DELIVERY",
               "DELIVERED", "FAILED"]
 
# The suite, as data. Each rule names a column, a check, the data-quality
# dimension it covers, and where in the pipeline the check belongs. Between them
# they cover four of the six dimensions: completeness, uniqueness, validity,
# consistency.
SUITE = [
    {"column": "scan_id",      "check": "not_null",  "dimension": "completeness", "where": "transform"},
    {"column": "scan_id",      "check": "unique",    "dimension": "uniqueness",   "where": "transform"},
    {"column": "scanned_at",   "check": "not_null",  "dimension": "completeness", "where": "transform"},
    {"column": "customer_id",  "check": "numeric",   "dimension": "validity",     "where": "transform"},
    {"column": "hub_id",       "check": "numeric",   "dimension": "validity",     "where": "transform"},
    {"column": "weight_kg",    "check": "in_range",  "min": 0, "max": 100,
                                                     "dimension": "validity",     "where": "transform"},
    {"column": "scan_type",    "check": "in_set",    "allowed": VALID_TYPES,
                                                     "dimension": "consistency",  "where": "transform"},
    {"column": "service_code", "check": "in_set",    "allowed": ["SDD", "EXP", "STD", "ECO"],
                                                     "dimension": "validity",     "where": "transform"},
    {"column": "scan_type",    "check": "uppercase", "dimension": "consistency",  "where": "transform"},
]
 
 
def run_check(df, rule):
    """Return the number of rows that break one rule. Every branch ends in a
    boolean Series that is True for the bad rows, so the counting is one line at
    the bottom. The else raises: a typo in a rule name must not pass silently."""
    col = rule["column"]
    check = rule["check"]
    s = df[col]
    if check == "not_null":
        bad = s.isna() | (s.astype(str).str.strip() == "")
    elif check == "unique":
        bad = s.duplicated(keep=False)
    elif check == "numeric":
        bad = pd.to_numeric(s, errors="coerce").isna() & s.notna()
    elif check == "in_range":
        n = pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False),
                          errors="coerce")
        bad = (n < rule["min"]) | (n > rule["max"])
    elif check == "in_set":
        bad = ~s.str.upper().isin([a.upper() for a in rule["allowed"]]) & s.notna()
    elif check == "uppercase":
        bad = s.notna() & (s != s.str.upper())
    else:
        raise ValueError("unknown check: " + check)
    return int(bad.sum())
 
 
def validate(df, suite):
    """Return a DataFrame: one row per rule, with how many rows failed it."""
    rows = []
    for rule in suite:
        rows.append({"column": rule["column"], "check": rule["check"],
                     "dimension": rule["dimension"],
                     "failed_rows": run_check(df, rule)})
    return pd.DataFrame(rows)
 
 
def check_schema(df, baseline_path):
    """Return (missing, new) against the column list stored at baseline_path.
    Missing and new columns are different severities, so return them separately
    rather than as one verdict."""
    with open(baseline_path) as handle:
        baseline = json.load(handle)
    missing = [c for c in baseline if c not in df.columns]
    new = [c for c in df.columns if c not in baseline]
    return missing, new
 
 
def reconcile(scan_date, engine):
    """Compare file rows against warehouse rows for one date. A gap is not a
    failure; an unexplained gap is. Return the file count, the warehouse count,
    the difference, and how many duplicates the file carried, so a human can see
    whether the gap is explained by dedup and rejects or not."""
    path = DATA_DIR + "/scans_" + scan_date + ".csv"
    try:
        raw = pd.read_csv(path, dtype=str)
    except FileNotFoundError:
        return {"scan_date": scan_date, "file_rows": None, "wh_rows": None,
                "diff": None, "file_dupes": None, "note": "file never arrived"}
    file_rows = len(raw)
    wh = pd.read_sql_query(
        "SELECT count(*) AS n FROM parcel_scans WHERE scan_date = %(d)s",
        engine, params={"d": scan_date})
    wh_rows = int(wh["n"][0])
    dupes = int(raw["scan_id"].duplicated().sum()) if "scan_id" in raw.columns else 0
    return {"scan_date": scan_date, "file_rows": file_rows, "wh_rows": wh_rows,
            "diff": file_rows - wh_rows, "file_dupes": dupes, "note": ""}
 
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
 
    day = pd.read_csv(DATA_DIR + "/scans_" + args.date + ".csv", dtype=str)
    report = validate(day, SUITE)
    print(report.to_string(index=False))
 
    # Exit non-zero if any rule failed, so this can gate a DAG task. A quality
    # script that always exits 0 cannot be a task in a DAG.
    total_failures = int(report["failed_rows"].sum())
    print("\ntotal failing rows:", total_failures)
    sys.exit(1 if total_failures > 0 else 0)