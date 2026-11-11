"""
Load data into HBase through the Thrift gateway (happybase).

  python hbase/load_hbase.py sessions data/raw --user-mod 10
      -> user_activity: ALL sessions of every 10th user (user_000000, user_000010, ...)

  python hbase/load_hbase.py product_daily output/product_daily_csv
      -> product_daily: per-product per-day metrics computed by spark/02_product_daily.py

Sampling note: we sample USERS, not sessions. Taking 10% of sessions at random would
give every user a 10%-complete activity stream, so "show me user X's history" would
be wrong for everyone. Taking 10% of users keeps each sampled stream complete.
The modulo spreads the sample across all pre-split regions.
"""
import argparse
import csv
import glob
import json
import os
import time
from datetime import datetime

import happybase

REV_BASE = 9_999_999_999


def connect(host, port):
    return happybase.Connection(host=host, port=port, timeout=120_000, autoconnect=True)


def ensure_tables(conn):
    existing = {t.decode() for t in conn.tables()}
    if "user_activity" not in existing:
        # happybase can't pass SPLITS; prefer hbase/schema.hbase. This is a fallback.
        conn.create_table("user_activity", {"s": dict(max_versions=1), "pv": dict(max_versions=1),
                                            "c": dict(max_versions=1)})
    if "product_daily" not in existing:
        conn.create_table("product_daily", {"m": dict(max_versions=1), "dv": dict(max_versions=1),
                                            "rf": dict(max_versions=1)})


def session_row(s):
    start = datetime.fromisoformat(s["start_time"])
    rev = REV_BASE - int(start.timestamp())
    key = f"{s['user_id']}#{rev:010d}#{s['session_id']}"
    cells = {
        b"s:start": s["start_time"].encode(),
        b"s:dur": str(s["duration_seconds"]).encode(),
        b"s:status": s["conversion_status"].encode(),
        b"s:ref": s["referrer"].encode(),
        b"s:dev": s["device_profile"]["type"].encode(),
        b"s:os": s["device_profile"]["os"].encode(),
        b"s:browser": s["device_profile"]["browser"].encode(),
        b"s:country": (s["geo_data"].get("country") or "").encode(),
        b"s:npv": str(len(s["page_views"])).encode(),
    }
    for i, pv in enumerate(s["page_views"]):
        # compact pipe-delimited value: page_type|product_id|category_id|view_seconds|offset_s
        off = int((datetime.fromisoformat(pv["timestamp"]) - start).total_seconds())
        cells[f"pv:{i:03d}".encode()] = (
            f"{pv['page_type']}|{pv['product_id'] or ''}|{pv['category_id'] or ''}|"
            f"{pv['view_duration']}|{off}").encode()
    for pid, c in s["cart_contents"].items():
        cells[f"c:{pid}".encode()] = f"{c['quantity']}|{c['price']}".encode()
    return key.encode(), cells


def load_sessions(conn, raw_dir, user_mod, batch_size):
    table = conn.table("user_activity")
    files = sorted(glob.glob(os.path.join(raw_dir, "sessions_*.json")),
                   key=lambda f: int(f.rsplit("_", 1)[1].split(".")[0]))
    t0, rows, cells = time.time(), 0, 0
    users = set()
    for f in files:
        chunk = json.load(open(f))
        with table.batch(batch_size=batch_size) as b:
            for s in chunk:
                if int(s["user_id"].split("_")[1]) % user_mod:
                    continue
                k, data = session_row(s)
                b.put(k, data)
                rows += 1
                cells += len(data)
                users.add(s["user_id"])
        print(f"  {os.path.basename(f)}: rows so far {rows:,}  ({time.time() - t0:.0f}s)", flush=True)
        del chunk
    dt = time.time() - t0
    print(f"user_activity: {rows:,} rows / {cells:,} cells for {len(users):,} users "
          f"in {dt:.0f}s ({rows / dt:,.0f} rows/s)")


def load_product_daily(conn, csv_dir, batch_size):
    table = conn.table("product_daily")
    t0, rows = time.time(), 0
    with table.batch(batch_size=batch_size) as b:
        for f in sorted(glob.glob(os.path.join(csv_dir, "*.csv"))):
            for r in csv.DictReader(open(f)):
                key = f"{r['product_id']}#{r['day'].replace('-', '')}".encode()
                data = {}
                for col in ["views", "view_seconds", "cart_adds", "orders", "units_sold", "revenue"]:
                    v = r.get(col)
                    if v not in (None, "", "0", "0.0"):       # sparse: zero counters are not stored
                        data[f"m:{col}".encode()] = v.encode()
                for col, val in r.items():
                    if col.startswith("dev_") and val not in ("", "0"):
                        data[f"dv:{col[4:]}".encode()] = val.encode()
                    elif col.startswith("ref_") and val not in ("", "0"):
                        data[f"rf:{col[4:]}".encode()] = val.encode()
                if data:
                    b.put(key, data)
                    rows += 1
    dt = time.time() - t0
    print(f"product_daily: {rows:,} rows in {dt:.0f}s ({rows / dt:,.0f} rows/s)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["sessions", "product_daily"])
    ap.add_argument("path")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--user-mod", type=int, default=10)
    ap.add_argument("--batch-size", type=int, default=2000)
    a = ap.parse_args()
    conn = connect(a.host, a.port)
    ensure_tables(conn)
    if a.what == "sessions":
        load_sessions(conn, a.path, a.user_mod, a.batch_size)
    else:
        load_product_daily(conn, a.path, a.batch_size)


if __name__ == "__main__":
    main()
