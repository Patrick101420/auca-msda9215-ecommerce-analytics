"""
Read from the operational stores (MongoDB, HBase) into Spark DataFrames.

We use the native Python drivers (pymongo, happybase) and push work down to each store
(a Mongo aggregation, an HBase prefix/range scan), so only the reduced result crosses into
Spark. The MongoDB Spark Connector / HBase-Spark connector would parallelise the read
across executors. That matters at production scale, but the connector jars must match
the exact Spark/Scala version (4.2/2.13 here). With our result sizes (thousands of rows)
a driver-side read is simpler and costs nothing measurable. Report section 5.3 covers this.
"""
import time

import happybase
from pymongo import MongoClient
from pyspark.sql import types as T

MONGO_URI = "mongodb://localhost:27017"


def mongo_users_with_summary(spark):
    """users + embedded purchase_summary (computed pattern) from MongoDB."""
    t0 = time.time()
    db = MongoClient(MONGO_URI).ecommerce
    rows = list(db.users.aggregate([
        {"$project": {
            "_id": 0, "user_id": "$_id", "country": "$geo.country",
            "registration_date": 1,
            "orders": {"$ifNull": ["$purchase_summary.orders", 0]},
            "total_spent": {"$ifNull": ["$purchase_summary.total_spent", 0.0]},
            "avg_order_value": {"$ifNull": ["$purchase_summary.avg_order_value", 0.0]},
            "first_purchase": "$purchase_summary.first_purchase",
            "last_purchase": "$purchase_summary.last_purchase",
        }}]))
    schema = T.StructType([
        T.StructField("user_id", T.StringType()), T.StructField("country", T.StringType()),
        T.StructField("registration_date", T.TimestampType()),
        T.StructField("orders", T.LongType()), T.StructField("total_spent", T.DoubleType()),
        T.StructField("avg_order_value", T.DoubleType()),
        T.StructField("first_purchase", T.TimestampType()), T.StructField("last_purchase", T.TimestampType())])
    df = spark.createDataFrame([(r["user_id"], r.get("country"), r.get("registration_date"),
                                 int(r["orders"]), float(r["total_spent"]), float(r["avg_order_value"]),
                                 r.get("first_purchase"), r.get("last_purchase")) for r in rows], schema)
    print(f"[mongo] users: {len(rows):,} docs in {time.time() - t0:.2f}s")
    return df


def hbase_user_engagement(spark, host="localhost", port=9090, before=None):
    """
    Per-user engagement from HBase user_activity, reading ONLY column family 's'.
    The pv family (page views, the bulk of the bytes) is never touched. This is the
    reason for splitting the families.

    before: ISO timestamp string. When given, a SingleColumnValueFilter on s:start runs
    inside the RegionServer, so sessions after the cutoff never cross the network.
    ISO-8601 strings sort lexicographically in time order, so a byte comparison is correct.
    """
    t0 = time.time()
    conn = happybase.Connection(host, port, timeout=300_000)
    tbl = conn.table("user_activity")
    flt = (f"SingleColumnValueFilter('s', 'start', <, 'binary:{before}', true, true)"
           if before else None)
    agg = {}
    n = 0
    for key, d in tbl.scan(columns=[b"s:dur", b"s:status", b"s:npv", b"s:start"], filter=flt,
                           batch_size=5000):
        uid = key.split(b"#", 1)[0].decode()
        a = agg.setdefault(uid, [0, 0, 0, 0, "9999", ""])
        a[0] += 1                                   # sessions
        a[1] += int(d[b"s:dur"])                    # total seconds
        a[2] += int(d[b"s:npv"])                    # page views
        a[3] += d[b"s:status"] == b"converted"
        st = d[b"s:start"].decode()
        a[4] = min(a[4], st)
        a[5] = max(a[5], st)
        n += 1
    conn.close()
    schema = T.StructType([T.StructField(c, t) for c, t in [
        ("user_id", T.StringType()), ("sessions", T.LongType()), ("total_session_seconds", T.LongType()),
        ("page_views", T.LongType()), ("converted_sessions", T.LongType()),
        ("first_session", T.StringType()), ("last_session", T.StringType())]])
    df = spark.createDataFrame([(u, *v) for u, v in agg.items()], schema)
    print(f"[hbase] user_activity: scanned {n:,} rows ({len(agg):,} users) in {time.time() - t0:.1f}s")
    return df


def hbase_product_daily(spark, host="localhost", port=9090, start_day=None, stop_day=None):
    """Full product_daily table (or a day window applied client-side) -> DataFrame."""
    t0 = time.time()
    conn = happybase.Connection(host, port, timeout=300_000)
    out = []
    for key, d in conn.table("product_daily").scan(columns=[b"m"], batch_size=10000):
        pid, day = key.decode().split("#")
        if (start_day and day < start_day) or (stop_day and day >= stop_day):
            continue
        g = lambda c: float(d.get(f"m:{c}".encode(), b"0"))
        out.append((pid, day, int(g("views")), int(g("cart_adds")), int(g("units_sold")),
                    int(g("orders")), g("revenue")))
    conn.close()
    schema = T.StructType([T.StructField(c, t) for c, t in [
        ("product_id", T.StringType()), ("day", T.StringType()), ("views", T.LongType()),
        ("cart_adds", T.LongType()), ("units_sold", T.LongType()), ("orders", T.LongType()),
        ("revenue", T.DoubleType())]])
    print(f"[hbase] product_daily: {len(out):,} rows in {time.time() - t0:.1f}s")
    return spark.createDataFrame(out, schema)
