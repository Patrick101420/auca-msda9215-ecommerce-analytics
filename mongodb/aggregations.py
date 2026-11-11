"""
MongoDB aggregation pipelines (Part 1). Each pipeline prints its result and execution time.
Results are saved to output/mongo_<name>.json.

P1 Revenue analytics   - monthly revenue by category, with each category's top-3 products
P2 User segmentation   - RFM-style segments from the embedded purchase_summary
P3 Product popularity  - most-viewed vs best-selling: $lookup from session journeys to orders

Also shows explain() for an indexed query to prove the index is used (IXSCAN vs COLLSCAN).
"""
import json
import os
import time
from datetime import timedelta

from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "output")
os.makedirs(OUT, exist_ok=True)
db = MongoClient("mongodb://localhost:27017").ecommerce


def run(name, coll, pipeline, show=10):
    t0 = time.perf_counter()
    res = list(db[coll].aggregate(pipeline, allowDiskUse=True))
    ms = (time.perf_counter() - t0) * 1000
    print(f"\n===== {name}  ({len(res)} rows, {ms:,.0f} ms) =====")
    for r in res[:show]:
        print(json.dumps(r, default=str))
    json.dump({"ms": round(ms), "rows": res}, open(os.path.join(OUT, f"mongo_{name}.json"), "w"),
              default=str, indent=1)
    return res


# ------------------------------------------------------------------ P1 revenue by category & month
# Line items embed the category (extended reference), so no $lookup to products is needed.
run("p1_revenue_by_category_month", "transactions", [
    {"$match": {"status": {"$in": ["completed", "shipped", "delivered", "processing"]}}},
    {"$unwind": "$items"},
    {"$group": {                                   # product-level revenue per month
        "_id": {"month": {"$dateTrunc": {"date": "$timestamp", "unit": "month"}},
                "category": "$items.category_name", "product": "$items.product_id",
                "product_name": "$items.product_name"},
        "revenue": {"$sum": "$items.subtotal"}, "units": {"$sum": "$items.quantity"}}},
    {"$sort": {"revenue": -1}},
    {"$group": {                                   # roll up to category-month, keep ranked products
        "_id": {"month": "$_id.month", "category": "$_id.category"},
        "revenue": {"$sum": "$revenue"}, "units": {"$sum": "$units"},
        "products": {"$push": {"product": "$_id.product_name", "revenue": {"$round": ["$revenue", 2]}}}}},
    {"$project": {"_id": 0, "month": {"$dateToString": {"date": "$_id.month", "format": "%Y-%m"}},
                  "category": "$_id.category", "revenue": {"$round": ["$revenue", 2]}, "units": 1,
                  "top3_products": {"$slice": ["$products", 3]}}},
    {"$sort": {"month": 1, "revenue": -1}},
])

# ------------------------------------------------------------------ P2 spend-quintile segmentation
# Reads only the users collection: the purchase summary is already embedded (computed pattern).
# First attempt used fixed RFM thresholds (e.g. "70+ orders & bought in last 7 days"). In this
# synthetic data every user buys ~50 times in 90 days, so fixed cut-offs gave 47 / 2,527 / 7,424 / 2
# users. $bucketAuto picks boundaries from the data (equal-sized quintiles of total spend).
latest = db.transactions.find_one(sort=[("timestamp", -1)])["timestamp"]
run("p2_user_segments", "users", [
    {"$match": {"purchase_summary.orders": {"$gt": 0}}},
    {"$project": {
        "spent": "$purchase_summary.total_spent", "orders": "$purchase_summary.orders",
        "aov": "$purchase_summary.avg_order_value", "discount": "$purchase_summary.total_discount",
        "recency_days": {"$dateDiff": {"startDate": "$purchase_summary.last_purchase",
                                       "endDate": latest, "unit": "hour"}}}},
    {"$bucketAuto": {"groupBy": "$spent", "buckets": 5, "output": {
        "users": {"$sum": 1}, "revenue": {"$sum": "$spent"},
        "avg_orders": {"$avg": "$orders"}, "avg_aov": {"$avg": "$aov"},
        "avg_recency_hours": {"$avg": "$recency_days"},
        "discount_share": {"$sum": "$discount"}}}},
    {"$project": {"_id": 0, "spend_from": {"$round": ["$_id.min", 0]}, "spend_to": {"$round": ["$_id.max", 0]},
                  "users": 1, "revenue": {"$round": ["$revenue", 0]},
                  "avg_orders": {"$round": ["$avg_orders", 1]}, "avg_aov": {"$round": ["$avg_aov", 2]},
                  "avg_recency_hours": {"$round": ["$avg_recency_hours", 1]},
                  "discount_pct_of_revenue": {"$round": [{"$multiply": [
                      {"$divide": ["$discount_share", "$revenue"]}, 100]}, 2]}}},
])

# Also the raw distribution of orders per user (used to choose the thresholds above)
run("p2b_orders_per_user_buckets", "users", [
    {"$bucket": {"groupBy": {"$ifNull": ["$purchase_summary.orders", 0]},
                 "boundaries": [0, 1, 30, 40, 50, 55, 60, 70, 1000], "default": "other",
                 "output": {"users": {"$sum": 1}, "spent": {"$sum": "$purchase_summary.total_spent"}}}},
])

# ------------------------------------------------------------------ P3 most viewed vs best selling
# Views are counted from the embedded page_views of a 30-day slice of session journeys
# (index on start_time), then $lookup-ed against orders for the same products.
since = latest - timedelta(days=30)
run("p3_viewed_vs_sold", "sessions", [
    {"$match": {"start_time": {"$gte": since}}},
    {"$unwind": "$page_views"},
    {"$match": {"page_views.page_type": "product_detail"}},
    {"$group": {"_id": "$page_views.product_id", "views": {"$sum": 1},
                "avg_view_seconds": {"$avg": "$page_views.view_duration"}}},
    {"$sort": {"views": -1}},
    {"$limit": 20},
    # localField/foreignField lets the join use the multikey index on items.product_id.
    # The first version used let + {$expr: {$in: [...]}}, which CANNOT use that index: 30.9 s
    # (kept in output/mongo_p3_viewed_vs_sold_SLOW_expr_lookup.json).
    {"$lookup": {
        "from": "transactions",
        "localField": "_id", "foreignField": "items.product_id",
        "let": {"pid": "$_id"},
        "pipeline": [
            {"$match": {"timestamp": {"$gte": since}}},
            {"$unwind": "$items"},
            {"$match": {"$expr": {"$eq": ["$items.product_id", "$$pid"]}}},
            {"$group": {"_id": None, "units": {"$sum": "$items.quantity"}, "revenue": {"$sum": "$items.subtotal"}}}],
        "as": "sales"}},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "p"}},
    {"$project": {"_id": 0, "product_id": "$_id", "name": {"$first": "$p.name"},
                  "category": {"$first": "$p.category.name"}, "views": 1,
                  "avg_view_seconds": {"$round": ["$avg_view_seconds", 0]},
                  "units_sold": {"$ifNull": [{"$first": "$sales.units"}, 0]},
                  "revenue": {"$round": [{"$ifNull": [{"$first": "$sales.revenue"}, 0]}, 2]},
                  "units_per_100_views": {"$round": [{"$multiply": [
                      {"$divide": [{"$ifNull": [{"$first": "$sales.units"}, 0]}, "$views"]}, 100]}, 2]}}},
])

# ------------------------------------------------------------------ index usage proof
uid = "user_000420"
for label, coll, flt, srt in [
    ("user's recent orders", "transactions", {"user_id": uid}, [("timestamp", -1)]),
    ("orders containing a product", "transactions", {"items.product_id": "prod_00042"}, None),
]:
    cur = db[coll].find(flt)
    if srt:
        cur = cur.sort(srt)
    ex = cur.limit(10).explain()
    stats = ex["executionStats"]
    plan = ex["queryPlanner"]["winningPlan"]
    stages = []
    while plan:
        stages.append(plan.get("stage"))
        plan = plan.get("inputStage")
    print(f"\nexplain[{label}]: stages={stages} keysExamined={stats['totalKeysExamined']} "
          f"docsExamined={stats['totalDocsExamined']} ms={stats['executionTimeMillis']}")
