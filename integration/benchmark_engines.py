"""
Same question, three engines: measured evidence for the technology-selection argument.

  QA (point/OLTP-style):    "Show user_000420's 5 most recent sessions"
  QB (analytical/scan):     "Top 20 most-viewed products in the last 30 days"

Each query is run 3 times. We report the best (warm) run, because the first call includes
connection setup / JVM warm-up that a long-running service would not pay.
Results -> output/benchmark.json
"""
import json
import os
import sys
import time
from datetime import timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "spark"))
from common import CLEAN, OUT, spark_session   # noqa: E402

import happybase                               # noqa: E402
from pymongo import MongoClient                # noqa: E402
from pyspark.sql import functions as F         # noqa: E402

USER = "user_000420"
db = MongoClient().ecommerce
hb = happybase.Connection("localhost", 9090, timeout=300_000)
spark = spark_session("bench")
latest = db.transactions.find_one(sort=[("timestamp", -1)])["timestamp"]
since = latest - timedelta(days=30)
since_day = since.strftime("%Y%m%d")


def best_of(fn, n=3):
    times, out = [], None
    for _ in range(n):
        t0 = time.perf_counter()
        out = fn()
        times.append(time.perf_counter() - t0)
    return round(min(times) * 1000, 1), out


res = {}
# ------------------------------------------------------------------ QA
res["QA_mongo_ms"], a1 = best_of(lambda: list(
    db.sessions.find({"user_id": USER}, {"start_time": 1, "conversion_status": 1})
    .sort("start_time", -1).limit(5)))
res["QA_hbase_ms"], a2 = best_of(lambda: list(
    hb.table("user_activity").scan(row_prefix=f"{USER}#".encode(), columns=[b"s"], limit=5)))
sess = spark.read.parquet(os.path.join(CLEAN, "sessions"))
res["QA_spark_ms"], a3 = best_of(lambda: sess.filter(F.col("user_id") == USER)
                                 .orderBy(F.desc("start_ts")).limit(5).collect())
res["QA_same_answer"] = ([d["_id"] for d in a1] == [k.decode().split("#")[2] for k, _ in a2]
                         == [r["session_id"] for r in a3])

# ------------------------------------------------------------------ QB
# Same semantics in every engine: count product-detail page views whose OWN timestamp is in
# the window. Sessions last <= 1 h, so start_time >= since - 1h lets the index narrow the scan.
pipeline = [{"$match": {"start_time": {"$gte": since - timedelta(hours=1)}}}, {"$unwind": "$page_views"},
            {"$match": {"page_views.page_type": "product_detail", "page_views.timestamp": {"$gte": since}}},
            {"$group": {"_id": "$page_views.product_id", "views": {"$sum": 1}}},
            {"$sort": {"views": -1, "_id": 1}}, {"$limit": 20}]
res["QB_mongo_ms"], b1 = best_of(lambda: list(db.sessions.aggregate(pipeline, allowDiskUse=True)), n=2)


def hbase_top():
    agg = {}
    # product-first rowkeys: a cross-product "last 30 days" question needs a FULL table scan,
    # filtering on the date part of the key client-side.
    for k, d in hb.table("product_daily").scan(columns=[b"m:views"], batch_size=20000):
        pid, day = k.decode().split("#")
        if day >= since_day:
            agg[pid] = agg.get(pid, 0) + int(d.get(b"m:views", b"0"))
    return sorted(agg.items(), key=lambda x: (-x[1], x[0]))[:20]


res["QB_hbase_ms"], b2 = best_of(hbase_top, n=2)

pv = spark.read.parquet(os.path.join(CLEAN, "page_views"))   # partitioned by event_date
# Bound passed as a STRING and cast inside Spark (session tz = UTC). Passing the naive Python
# datetime directly made PySpark interpret it in the OS zone (EAT, UTC+3): a silent 3-hour shift.
since_ts = F.lit(since.isoformat(sep=" ")).cast("timestamp")
qb = (pv.filter((F.col("event_date") >= since.date()) & (F.col("ts") >= since_ts)
                & (F.col("page_type") == "product_detail"))
      .groupBy("product_id").count().orderBy(F.desc("count"), "product_id").limit(20))
res["QB_spark_ms"], b3 = best_of(lambda: qb.collect())
res["QB_spark_partitions_pruned"] = "PartitionFilters" in qb._jdf.queryExecution().executedPlan().toString()
res["QB_mongo_vs_spark_same_top20"] = [d["_id"] for d in b1] == [r["product_id"] for r in b3]
res["QB_hbase_note"] = ("HBase aggregates by DAY (calendar-day boundary), Mongo/Spark by exact timestamp; "
                        "top-20 may differ at the edges")

print(json.dumps(res, indent=2))
json.dump(res, open(os.path.join(OUT, "benchmark.json"), "w"), indent=2)
spark.stop()
