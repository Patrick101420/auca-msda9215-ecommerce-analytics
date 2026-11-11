"""
Spark batch job 2: per-product, per-day performance metrics -> HBase product_daily.

This is the Spark -> HBase handoff: Spark does the heavy aggregation over every page view
(~20M rows) and transaction line. HBase then serves cheap point and time-range reads per
product (rowkey product_id#yyyymmdd).

Output: data/clean/product_daily (Parquet) and output/product_daily_csv (single CSV for the
happybase loader).
"""
import os

from pyspark.sql import functions as F

from common import CLEAN, OUT, spark_session

spark = spark_session("02-product-daily")
rd = lambda n: spark.read.parquet(os.path.join(CLEAN, n))

pv = rd("page_views").filter(F.col("page_type") == "product_detail")

# Views: one pass, with conditional sums for the sparse device/referrer breakdown.
# Pivot with an explicit value list avoids an extra job to discover distinct values.
devices = ["mobile", "desktop", "tablet"]
referrers = ["direct", "email", "social", "search_engine", "affiliate"]
views = (pv.groupBy("product_id", F.col("event_date").alias("day"))
         .agg(F.count("*").alias("views"),
              F.sum("view_seconds").alias("view_seconds"),
              *[F.sum(F.when(F.col("device_type") == d, 1).otherwise(0)).alias(f"dev_{d}") for d in devices],
              *[F.sum(F.when(F.col("referrer") == r, 1).otherwise(0)).alias(f"ref_{r}") for r in referrers]))

# Cart adds: the generator records cart lines per session without a timestamp, so they
# are attributed to the session start date (stated as an assumption in the report).
carts = (rd("cart_items").join(rd("sessions").select("session_id", "user_id", "session_date"),
                              ["session_id", "user_id"])   # composite key: session_id alone collides
         .groupBy("product_id", F.col("session_date").alias("day"))
         .agg(F.sum("quantity").alias("cart_adds")))

sales = (rd("transaction_items")
         .groupBy("product_id", F.to_date("ts").alias("day"))
         .agg(F.countDistinct("transaction_id").alias("orders"),
              F.sum("quantity").alias("units_sold"),
              F.round(F.sum("subtotal"), 2).alias("revenue")))

daily = (views.join(carts, ["product_id", "day"], "full_outer")
         .join(sales, ["product_id", "day"], "full_outer")
         .na.fill(0)
         .orderBy("product_id", "day"))

daily.write.mode("overwrite").parquet(os.path.join(CLEAN, "product_daily"))
# coalesce(1): ~400k small rows; one file keeps the HBase loader simple
(spark.read.parquet(os.path.join(CLEAN, "product_daily")).coalesce(1)
 .write.mode("overwrite").option("header", True).csv(os.path.join(OUT, "product_daily_csv")))

d = spark.read.parquet(os.path.join(CLEAN, "product_daily"))
print("product_daily rows:", d.count())
d.agg(F.countDistinct("product_id").alias("products"), F.min("day"), F.max("day"),
      F.sum("views"), F.sum("cart_adds"), F.sum("units_sold"), F.round(F.sum("revenue"), 2)).show(truncate=False)
spark.stop()
