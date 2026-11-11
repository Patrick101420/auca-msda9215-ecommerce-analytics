"""
Load the generated e-commerce JSON into MongoDB using the document model designed
in report section 3.1.

Collections
-----------
categories    25 docs.   Subcategories EMBEDDED (bounded: 3-5 per category).
products      5k docs.   Category reference + EXTENDED REFERENCE (category name copied in),
                         price_history EMBEDDED (bounded: 1-3 entries).
users         10k docs.  Profile + COMPUTED purchase_summary + SUBSET of the 5 most
                         recent orders (pipeline in build_user_summaries.js / below).
transactions  500k docs. Line items EMBEDDED. Each item carries a snapshot of
                         product name + category (extended reference), so revenue-by-category
                         needs no $lookup.
sessions      2M docs.   Whole journey (page_views, cart) EMBEDDED. One read = one journey.

Usage:  python mongodb/load_mongo.py data/raw  [--sessions-files N]
"""
import argparse
import glob
import json
import os
import sys
import time
from datetime import datetime

from pymongo import ASCENDING, DESCENDING, MongoClient, InsertOne
from pymongo.errors import BulkWriteError

BATCH = 10_000


def ts(s):
    """ISO string -> datetime (Mongo stores BSON dates; enables $dateTrunc, range indexes)."""
    return datetime.fromisoformat(s) if s else None


def bulk_insert(coll, docs, label):
    t0, n, buf = time.time(), 0, []
    for d in docs:
        buf.append(InsertOne(d))
        if len(buf) >= BATCH:
            n += _flush(coll, buf)
            buf = []
    if buf:
        n += _flush(coll, buf)
    dt = time.time() - t0
    print(f"  {label:<13} {n:>10,} docs  {dt:6.1f}s  ({n / max(dt, 1e-9):,.0f} docs/s)")
    return n


def _flush(coll, buf):
    try:
        # ordered=False: the server can parallelise and one bad doc doesn't abort the batch
        return coll.bulk_write(buf, ordered=False).inserted_count
    except BulkWriteError as e:  # duplicate _id etc. - report, don't die
        print(f"    bulk write errors: {len(e.details['writeErrors'])}", file=sys.stderr)
        return e.details["nInserted"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("raw_dir")
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default="ecommerce")
    ap.add_argument("--sessions-files", type=int, default=None,
                    help="load only the first N sessions_*.json files (default: all)")
    args = ap.parse_args()

    db = MongoClient(args.uri)[args.db]
    for c in ["categories", "products", "users", "transactions", "sessions"]:
        db[c].drop()
    print("Loading into MongoDB:", args.uri, args.db)

    # ---------- categories ----------
    cats = json.load(open(os.path.join(args.raw_dir, "categories.json")))
    cat_by_id = {}
    cat_docs = []
    for c in cats:
        margins = [s["profit_margin"] for s in c["subcategories"]]
        doc = {
            "_id": c["category_id"],              # natural key as _id: no extra index needed
            "name": c["name"],
            "subcategories": c["subcategories"],   # embedded: bounded and always read with parent
            # products in the data carry no subcategory_id, so the only defensible margin at
            # product level is the category mean. Stored once here, flagged as an estimate.
            "avg_profit_margin_est": round(sum(margins) / len(margins), 4),
        }
        cat_by_id[c["category_id"]] = doc
        cat_docs.append(doc)
    bulk_insert(db.categories, cat_docs, "categories")

    # ---------- products ----------
    prods = json.load(open(os.path.join(args.raw_dir, "products.json")))
    prod_by_id = {}
    prod_docs = []
    for p in prods:
        cat = cat_by_id.get(p["category_id"], {})
        doc = {
            "_id": p["product_id"],
            "name": p["name"],
            "category": {                          # extended reference
                "category_id": p["category_id"],
                "name": cat.get("name"),
            },
            "subcategory_id": p.get("subcategory_id"),   # absent in generated data -> null
            "price": p["base_price"],
            "current_stock": p["current_stock"],
            "is_active": p["is_active"],
            "price_history": [{"price": h["price"], "date": ts(h["date"])} for h in p["price_history"]],
            "creation_date": ts(p["creation_date"]),
        }
        prod_by_id[p["product_id"]] = doc
        prod_docs.append(doc)
    bulk_insert(db.products, prod_docs, "products")

    # ---------- users (profile only; summaries are computed by an aggregation below) ----------
    users = json.load(open(os.path.join(args.raw_dir, "users.json")))
    bulk_insert(db.users, ({
        "_id": u["user_id"],
        "geo": u["geo_data"],
        "registration_date": ts(u["registration_date"]),
        "last_active": ts(u["last_active"]),
    } for u in users), "users")

    # ---------- transactions ----------
    txns = json.load(open(os.path.join(args.raw_dir, "transactions.json")))

    def txn_doc(t):
        items = []
        for it in t["items"]:
            p = prod_by_id.get(it["product_id"], {})
            items.append({
                **it,
                "product_name": p.get("name"),
                "category_id": p.get("category", {}).get("category_id"),
                "category_name": p.get("category", {}).get("name"),
            })
        return {
            "_id": t["transaction_id"],
            "session_id": t["session_id"],
            "user_id": t["user_id"],
            "timestamp": ts(t["timestamp"]),
            "items": items,
            "subtotal": t["subtotal"],
            "discount": t["discount"],
            "total": t["total"],
            "payment_method": t["payment_method"],
            "status": t["status"],
            "channel": "web_session" if t["session_id"] else "unlinked",
        }
    bulk_insert(db.transactions, (txn_doc(t) for t in txns), "transactions")
    del txns

    # ---------- sessions ----------
    files = sorted(glob.glob(os.path.join(args.raw_dir, "sessions_*.json")),
                   key=lambda f: int(f.rsplit("_", 1)[1].split(".")[0]))
    if args.sessions_files:
        files = files[: args.sessions_files]
    total = 0
    for f in files:
        chunk = json.load(open(f))
        total += bulk_insert(db.sessions, ({
            "_id": s["session_id"],
            "user_id": s["user_id"],
            "start_time": ts(s["start_time"]),
            "end_time": ts(s["end_time"]),
            "duration_seconds": s["duration_seconds"],
            "geo": s["geo_data"],
            "device": s["device_profile"],
            "referrer": s["referrer"],
            "conversion_status": s["conversion_status"],
            "viewed_products": s["viewed_products"],
            "page_views": [{**pv, "timestamp": ts(pv["timestamp"])} for pv in s["page_views"]],
            # cart as an array (not a dict keyed by product_id): field names that are data
            # values cannot be indexed or $unwind-ed.
            "cart": [{"product_id": k, **v} for k, v in s["cart_contents"].items()],
        } for s in chunk), os.path.basename(f))
        del chunk
    print(f"  sessions total {total:,}")

    # ---------- indexes (each one maps to a query in queries/ or the integration layer) ----------
    t0 = time.time()
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])   # a user's orders, newest first
    db.transactions.create_index([("timestamp", ASCENDING)])                          # revenue over a period
    db.transactions.create_index([("items.product_id", ASCENDING)])                   # multikey: orders containing X
    db.sessions.create_index([("user_id", ASCENDING), ("start_time", DESCENDING)])
    db.sessions.create_index([("start_time", ASCENDING)])
    db.products.create_index([("category.category_id", ASCENDING)])
    print(f"  indexes built in {time.time() - t0:.1f}s")

    # ---------- computed pattern: embed purchase summary into each user ----------
    t0 = time.time()
    db.transactions.aggregate([
        {"$sort": {"user_id": 1, "timestamp": -1}},
        {"$group": {
            "_id": "$user_id",
            "orders": {"$sum": 1},
            "total_spent": {"$sum": "$total"},
            "total_discount": {"$sum": "$discount"},
            "first_purchase": {"$min": "$timestamp"},
            "last_purchase": {"$max": "$timestamp"},
            "recent": {"$push": {"transaction_id": "$_id", "timestamp": "$timestamp", "total": "$total"}},
        }},
        {"$project": {
            "purchase_summary": {
                "orders": "$orders",
                "total_spent": {"$round": ["$total_spent", 2]},
                "avg_order_value": {"$round": [{"$divide": ["$total_spent", "$orders"]}, 2]},
                "total_discount": {"$round": ["$total_discount", 2]},
                "first_purchase": "$first_purchase",
                "last_purchase": "$last_purchase",
                "computed_at": "$$NOW",
            },
            "recent_orders": {"$slice": ["$recent", 5]},  # subset pattern: bounded array
        }},
        {"$merge": {"into": "users", "on": "_id", "whenMatched": "merge", "whenNotMatched": "discard"}},
    ], allowDiskUse=True)
    n = db.users.count_documents({"purchase_summary": {"$exists": True}})
    print(f"  purchase_summary embedded into {n:,} users in {time.time() - t0:.1f}s")

    for c in ["categories", "products", "users", "transactions", "sessions"]:
        st = db.command("collStats", c)
        print(f"  {c:<13} count={st['count']:>10,}  data={st['size'] / 1e6:8.1f} MB  "
              f"storage={st['storageSize'] / 1e6:8.1f} MB  indexes={st['totalIndexSize'] / 1e6:6.1f} MB")


if __name__ == "__main__":
    main()
