"""
Spark batch job 1: clean and normalise the raw JSON into typed, analysis-ready Parquet tables.

Raw input (data/raw)                    Clean output (data/clean, Parquet)
--------------------------------------  ---------------------------------------------
users.json                              users
categories.json                         categories, subcategories
products.json                           products, price_history
transactions.json                       transactions, transaction_items
sessions_*.json (20 x ~100k)            sessions, page_views (partitioned by event_date), cart_items

Design choices (see report section 4.1):
* EXPLICIT SCHEMAS instead of inference. Inference needs an extra full pass over ~GBs of
  JSON. Worse, sessions.cart_contents is a dict keyed by product_id, so inference would build
  a struct with one field per product id ever seen (thousands of columns). Declaring it as
  MapType<string, struct> keeps it a single column that we can explode().
* multiLine=True because the generator writes JSON arrays, not JSON Lines. Each file is
  then read whole by one task. That's acceptable at 20 files, and it's why our first write
  goes to a splittable columnar format.
* Data-quality rules are counted, not silently fixed, and written to output/dq_report.json.
"""
import json
import os
import time

from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import CLEAN, OUT, RAW, spark_session

t_start = time.time()
spark = spark_session("01-clean-normalize")
os.makedirs(OUT, exist_ok=True)

# ----------------------------------------------------------------- schemas
geo = T.StructType([T.StructField(c, T.StringType()) for c in ["city", "state", "country"]])
users_schema = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("geo_data", geo),
    T.StructField("registration_date", T.StringType()),
    T.StructField("last_active", T.StringType()),
])
cat_schema = T.StructType([
    T.StructField("category_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("subcategories", T.ArrayType(T.StructType([
        T.StructField("subcategory_id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("profit_margin", T.DoubleType()),
    ]))),
])
prod_schema = T.StructType([
    T.StructField("product_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("category_id", T.StringType()),
    T.StructField("subcategory_id", T.StringType()),     # documented in brief, absent in data
    T.StructField("base_price", T.DoubleType()),
    T.StructField("current_stock", T.IntegerType()),
    T.StructField("is_active", T.BooleanType()),
    T.StructField("price_history", T.ArrayType(T.StructType([
        T.StructField("price", T.DoubleType()),
        T.StructField("date", T.StringType()),
    ]))),
    T.StructField("creation_date", T.StringType()),
])
item_schema = T.StructType([
    T.StructField("product_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("unit_price", T.DoubleType()),
    T.StructField("subtotal", T.DoubleType()),
])
txn_schema = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("items", T.ArrayType(item_schema)),
    T.StructField("subtotal", T.DoubleType()),
    T.StructField("discount", T.DoubleType()),
    T.StructField("total", T.DoubleType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("status", T.StringType()),
])
session_schema = T.StructType([
    T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("start_time", T.StringType()),
    T.StructField("end_time", T.StringType()),
    T.StructField("duration_seconds", T.IntegerType()),
    T.StructField("geo_data", T.StructType(geo.fields + [T.StructField("ip_address", T.StringType())])),
    T.StructField("device_profile", T.StructType([
        T.StructField(c, T.StringType()) for c in ["type", "os", "browser"]])),
    T.StructField("viewed_products", T.ArrayType(T.StringType())),
    T.StructField("page_views", T.ArrayType(T.StructType([
        T.StructField("timestamp", T.StringType()),
        T.StructField("page_type", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("category_id", T.StringType()),
        T.StructField("view_duration", T.IntegerType()),
    ]))),
    T.StructField("cart_contents", T.MapType(T.StringType(), T.StructType([
        T.StructField("quantity", T.IntegerType()),
        T.StructField("price", T.DoubleType()),
    ]))),
    T.StructField("conversion_status", T.StringType()),
    T.StructField("referrer", T.StringType()),
])


def read(name, schema):
    return spark.read.schema(schema).option("multiLine", True).json(os.path.join(RAW, name))


def write(df, name, partition_by=None):
    w = df.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.parquet(os.path.join(CLEAN, name))


def norm_str(c):
    """trim + empty-string -> NULL: one canonical representation of 'missing'."""
    return F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))


dq = {}

# ----------------------------------------------------------------- users
users_raw = read("users.json", users_schema)
users = (users_raw
         .select("user_id",
                 norm_str("geo_data.city").alias("city"),
                 F.upper(norm_str("geo_data.state")).alias("state"),
                 F.upper(norm_str("geo_data.country")).alias("country"),
                 F.to_timestamp("registration_date").alias("registration_date"),
                 F.to_timestamp("last_active").alias("last_active"))
         .dropDuplicates(["user_id"]))
dq["users"] = {
    "rows_raw": users_raw.count(),
    "rows_clean": users.count(),
    "null_registration_date": users.filter(F.col("registration_date").isNull()).count(),
    # The generator draws country from all ISO codes but state from US states: a
    # geo-consistency (veracity) issue worth reporting rather than "fixing".
    "non_us_country_with_us_state": users.filter(F.col("country") != "US").count(),
}
write(users, "users")

# ----------------------------------------------------------------- categories
cats = read("categories.json", cat_schema).cache()
write(cats.select("category_id", F.col("name").alias("category_name")), "categories")
subcats = (cats.select("category_id", F.explode("subcategories").alias("s"))
           .select("category_id", "s.subcategory_id", F.col("s.name").alias("subcategory_name"),
                   "s.profit_margin"))
write(subcats, "subcategories")

# ----------------------------------------------------------------- products
prods_raw = read("products.json", prod_schema).cache()
products = prods_raw.select(
    "product_id", "name", "category_id", "subcategory_id",
    F.col("base_price").alias("current_price"), "current_stock", "is_active",
    F.to_timestamp("creation_date").alias("creation_date"),
    F.size("price_history").alias("n_price_changes"))
price_history = (prods_raw
                 .select("product_id", F.posexplode("price_history").alias("seq", "ph"))
                 .select("product_id", "seq", F.col("ph.price").alias("price"),
                         F.to_timestamp("ph.date").alias("valid_from")))
dq["products"] = {
    "rows": products.count(),
    "missing_subcategory_id": products.filter(F.col("subcategory_id").isNull()).count(),
    "inactive": products.filter(~F.col("is_active")).count(),
    "out_of_stock": products.filter(F.col("current_stock") <= 0).count(),
}
write(products, "products")
write(price_history, "price_history")

# ----------------------------------------------------------------- transactions
txn_raw = read("transactions.json", txn_schema)
txns = (txn_raw
        .withColumn("ts", F.to_timestamp("timestamp"))
        .withColumn("session_id", norm_str("session_id"))
        .withColumn("items_sum", F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["subtotal"]))
        .withColumn("n_items", F.size("items"))
        .withColumn("units", F.aggregate("items", F.lit(0), lambda acc, x: acc + x["quantity"])))
txns = txns.dropDuplicates(["transaction_id"]).cache()   # reused by 3 outputs + DQ checks
dq["transactions"] = {
    "rows": txns.count(),
    "unlinked_to_session": txns.filter(F.col("session_id").isNull()).count(),
    # arithmetic integrity (1 cent tolerance for float rounding)
    "subtotal_ne_sum_items": txns.filter(F.abs(F.col("subtotal") - F.col("items_sum")) > 0.01).count(),
    "total_ne_subtotal_minus_discount": txns.filter(
        F.abs(F.col("total") - (F.col("subtotal") - F.col("discount"))) > 0.01).count(),
    "empty_items": txns.filter(F.col("n_items") == 0).count(),
}
write(txns.select("transaction_id", "session_id", "user_id", "ts",
                  F.to_date("ts").alias("txn_date"), "n_items", "units",
                  F.round("subtotal", 2).alias("subtotal"), "discount", "total",
                  "payment_method", "status",
                  F.when(F.col("session_id").isNull(), "unlinked").otherwise("web_session").alias("channel")),
      "transactions")
write(txns.select("transaction_id", "user_id", "ts", F.posexplode("items").alias("line_no", "it"))
      .select("transaction_id", "user_id", "ts", "line_no", "it.product_id", "it.quantity",
              "it.unit_price", "it.subtotal"),
      "transaction_items")

# ----------------------------------------------------------------- sessions
sess_raw = read("sessions_*.json", session_schema)
# Persist: the parsed sessions feed THREE writes (sessions, page_views, cart_items) and the
# DQ counts. Without persist, each action would re-parse ~GBs of JSON (branching lineage,
# Lecture 4's "good" caching case). MEMORY_AND_DISK because it may not fit in 10 GB.
from pyspark import StorageLevel
sess = (sess_raw
        .withColumn("start_ts", F.to_timestamp("start_time"))
        .withColumn("end_ts", F.to_timestamp("end_time"))
        # session_id is uuid4().hex[:10] (40 bits): at 2M sessions ~2 birthday collisions are
        # expected, and we observed exactly 2, between DIFFERENT users. Deduplicating on
        # session_id alone would delete real sessions, so the natural key is (session_id, user_id).
        .dropDuplicates(["session_id", "user_id"])
        .persist(StorageLevel.MEMORY_AND_DISK))

page_types = F.transform("page_views", lambda x: x["page_type"])
sessions = sess.select(
    "session_id", "user_id", "start_ts", "end_ts", "duration_seconds",
    F.to_date("start_ts").alias("session_date"),
    F.upper("geo_data.country").alias("country"),
    F.col("device_profile.type").alias("device_type"),
    F.col("device_profile.os").alias("os"),
    F.col("device_profile.browser").alias("browser"),
    "referrer", "conversion_status",
    F.size("page_views").alias("n_page_views"),
    F.size("viewed_products").alias("n_products_viewed"),
    F.size(F.map_keys("cart_contents")).alias("n_cart_lines"),
    F.aggregate(F.map_values("cart_contents"), F.lit(0.0),
                lambda acc, v: acc + v["quantity"] * v["price"]).alias("cart_value"),
    F.array_contains(page_types, "product_detail").alias("reached_product"),
    F.array_contains(page_types, "cart").alias("reached_cart"),
    F.array_contains(page_types, "checkout").alias("reached_checkout"),
    F.array_contains(page_types, "confirmation").alias("reached_confirmation"),
    page_types.getItem(0).alias("landing_page"),
)
page_views = (sess.select("session_id", "user_id", "device_profile.type", "referrer",
                          F.posexplode("page_views").alias("seq", "pv"))
              .select("session_id", "user_id", F.col("type").alias("device_type"), "referrer", "seq",
                      F.to_timestamp("pv.timestamp").alias("ts"),
                      F.to_date(F.to_timestamp("pv.timestamp")).alias("event_date"),
                      "pv.page_type", "pv.product_id", "pv.category_id",
                      F.col("pv.view_duration").alias("view_seconds")))
cart_items = (sess.select("session_id", "user_id", F.explode("cart_contents").alias("product_id", "c"))
              .select("session_id", "user_id", "product_id", "c.quantity", F.col("c.price").alias("price")))

write(sessions, "sessions")
write(page_views, "page_views", partition_by="event_date")   # date-pruned scans downstream
write(cart_items, "cart_items")

s_clean = spark.read.parquet(os.path.join(CLEAN, "sessions"))
dq["sessions"] = {
    "rows": s_clean.count(),
    "status_counts": {r["conversion_status"]: r["count"]
                      for r in s_clean.groupBy("conversion_status").count().collect()},
    "duration_mismatch": sess.filter(
        (F.col("end_ts").cast("long") - F.col("start_ts").cast("long")) != F.col("duration_seconds")).count(),
    "checkout_without_cart": s_clean.filter(F.col("reached_checkout") & (F.col("n_cart_lines") == 0)).count(),
}
# "converted" sessions that never produced a transaction: once the 500k transaction target
# is reached, or when a stock check fails, the generator still labels the session converted.
linked = spark.read.parquet(os.path.join(CLEAN, "transactions")).filter(F.col("session_id").isNotNull())
dq["sessions"]["converted_without_transaction"] = (
    s_clean.filter(F.col("conversion_status") == "converted")
    .join(linked.select("session_id", "user_id"), ["session_id", "user_id"], "left_anti").count())
dq["sessions"]["session_id_collisions"] = (s_clean.groupBy("session_id").count()
                                           .filter("count > 1").count())
dq["page_views"] = {"rows": spark.read.parquet(os.path.join(CLEAN, "page_views")).count()}
dq["runtime_seconds"] = round(time.time() - t_start, 1)

json.dump(dq, open(os.path.join(OUT, "dq_report.json"), "w"), indent=2, default=str)
print(json.dumps(dq, indent=2, default=str))
spark.stop()
