"""
Integrated query 2: Where is demand being lost? Stock-outs vs weak conversion.

BUSINESS QUESTION
  "How much revenue did we lose because products ran out of stock, and which products that
   stayed in stock under-convert relative to their category?"

DATA SOURCES
  HBase    product_daily  per-product daily views / cart adds / units (wide-column time series)
  MongoDB  products       current_stock, is_active, price, category (operational catalogue)
  Spark    joins, category-level medians (window percentile), cross-sectional estimates

HOW WE GOT HERE (three attempts: the reasoning is part of the result)
  v1  "Leakage vs category median units-per-view" over ALL products gave $126M (26% of revenue).
      Rejected. Half the catalogue sold out. Sold-out products lose exposure (views) but keep
      the sales they made, so their units-per-view is inflated (0.47 vs 0.21). That inflates
      category medians and makes every in-stock product look "leaky". Confounded by inventory.
  v2  Event study in calendar time: find each product's sell-out day from the HBase daily series,
      compare views/day before vs after. Rejected. Views of sold-out products are spread evenly
      across the 90 days (2nd-half/1st-half ratio 1.02, same as in-stock products), and "sell-out
      days" cluster at the end of the window. The generator depletes stock in GENERATION order but
      stamps each session with a RANDOM date, so stock-outs leave no trace in the timeline. The
      data's inventory history is inconsistent with its own clock (a veracity finding).
  v3  (this file) Cross-sectional comparison within category, which the data CAN support:
        missing views(p)  = median views of in-stock products in p's category - views(p)
        lost units(p)     = missing views(p) * median units-per-view of in-stock products in category
        lost revenue(p)   = lost units(p) * price(p)
      Assumes a sold-out product would have attracted and converted traffic like an in-stock
      peer in its category. Its own (inflated) conversion is deliberately NOT used.
      Conversion leakage is computed only among in-stock products vs in-stock medians.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "spark"))
from common import OUT, spark_session                     # noqa: E402
from stores import MONGO_URI, hbase_product_daily         # noqa: E402

from pymongo import MongoClient                           # noqa: E402
from pyspark.sql import Window                            # noqa: E402
from pyspark.sql import functions as F                    # noqa: E402

spark = spark_session("07-demand-leakage")

t0 = time.time()
daily = hbase_product_daily(spark).withColumn("d", F.to_date("day", "yyyyMMdd")).cache()
t_hbase = time.time() - t0

t0 = time.time()
prods = list(MongoClient(MONGO_URI).ecommerce.products.find(
    {}, {"price": 1, "current_stock": 1, "is_active": 1, "category": 1, "name": 1}))
t_mongo = time.time() - t0
catalog = spark.createDataFrame(
    [(p["_id"], p["name"], p["category"]["name"], float(p["price"]), int(p["current_stock"]),
      bool(p["is_active"])) for p in prods],
    "product_id string, name string, category_name string, price double, current_stock int, is_active boolean")

# ------------------------------------------------ per-product totals (HBase series) + catalogue (Mongo)
lo, hi = daily.agg(F.min("d"), F.max("d")).first()
mid = lo + (hi - lo) / 2
perf = daily.groupBy("product_id").agg(
    F.sum("views").alias("views"), F.sum("cart_adds").alias("cart_adds"),
    F.sum("units_sold").alias("units"), F.sum("revenue").alias("revenue"),
    F.sum(F.when(F.col("d") < F.lit(mid), F.col("views")).otherwise(0)).alias("views_h1"),
    F.sum(F.when(F.col("d") >= F.lit(mid), F.col("views")).otherwise(0)).alias("views_h2"))
p = (catalog.join(perf, "product_id", "left").na.fill(0)
     .withColumn("state", F.when(~F.col("is_active"), "inactive")
                 .when(F.col("current_stock") == 0, "sold_out").otherwise("in_stock"))
     .withColumn("upv", F.when(F.col("views") > 0, F.col("units") / F.col("views"))))

# evidence table for the report (v1/v2 rejection)
evidence = (p.groupBy("state").agg(
    F.count("*").alias("products"), F.round(F.avg("views"), 0).alias("avg_views"),
    F.round(F.avg("units"), 0).alias("avg_units"), F.round(F.avg("upv"), 3).alias("avg_units_per_view"),
    F.round(F.sum("views_h2") / F.sum("views_h1"), 3).alias("views_h2_over_h1"),
    F.round(F.sum("revenue"), 0).alias("revenue")).orderBy("state"))
evidence.show()
evidence.toPandas().to_csv(os.path.join(OUT, "stock_state_evidence.csv"), index=False)

# ------------------------------------------------ in-stock benchmarks per category
bench = (p.filter("state = 'in_stock' AND views > 0").groupBy("category_name")
         .agg(F.percentile_approx("views", 0.5).alias("cat_instock_median_views"),
              F.percentile_approx("upv", 0.5).alias("cat_instock_median_upv")))
p = p.join(bench, "category_name", "left")

# ------------------------------------------------ v3a: lost to stock-outs
so = (p.filter("state = 'sold_out'")
      .withColumn("missing_views", F.greatest(F.lit(0), F.col("cat_instock_median_views") - F.col("views")))
      .withColumn("lost_units", F.col("missing_views") * F.col("cat_instock_median_upv"))
      .withColumn("lost_revenue", F.round(F.col("lost_units") * F.col("price"), 2)).cache())

# ------------------------------------------------ v3b: conversion leakage among in-stock products
ins = (p.filter("state = 'in_stock' AND views > 0")
       .withColumn("lost_units", F.greatest(F.lit(0.0), F.col("views") * (F.col("cat_instock_median_upv") - F.col("upv"))))
       .withColumn("lost_revenue", F.round(F.col("lost_units") * F.col("price"), 2)).cache())

cols = ["product_id", "category_name", "price", "views", "units", F.round("upv", 3).alias("upv"),
        "cat_instock_median_views", F.round("cat_instock_median_upv", 3).alias("cat_med_upv"),
        F.round("lost_units", 1).alias("lost_units"), "lost_revenue"]
print("Largest estimated losses from stock-outs"); so.select(*cols).orderBy(F.desc("lost_revenue")).show(10, truncate=28)
print("In-stock products converting furthest below category peers"); ins.select(*cols).orderBy(F.desc("lost_revenue")).show(10, truncate=28)

by_cat = (p.groupBy("category_name").agg(
            F.count("*").alias("products"),
            F.sum(F.when(F.col("state") == "sold_out", 1).otherwise(0)).alias("sold_out"),
            F.round(F.sum("revenue"), 0).alias("revenue"))
          .join(so.groupBy("category_name").agg(F.round(F.sum("lost_revenue"), 0).alias("stockout_loss")), "category_name", "left")
          .join(ins.groupBy("category_name").agg(F.round(F.sum("lost_revenue"), 0).alias("conversion_leak")), "category_name", "left")
          .na.fill(0)
          .withColumn("sold_out_pct", F.round(100 * F.col("sold_out") / F.col("products"), 1))
          .withColumn("stockout_loss_pct_of_rev", F.round(100 * F.col("stockout_loss") / F.col("revenue"), 1))
          .orderBy(F.desc("stockout_loss")))
by_cat.show(25, truncate=False)

p.select("product_id", "name", "category_name", "state", "price", "current_stock", "views", "cart_adds", "units",
         "upv", "cat_instock_median_views", "cat_instock_median_upv").toPandas() \
    .to_csv(os.path.join(OUT, "product_leakage.csv"), index=False)
so.select("product_id", "category_name", "price", "views", "units", "missing_views", "lost_units", "lost_revenue") \
    .toPandas().to_csv(os.path.join(OUT, "stockout_impact.csv"), index=False)
ins.select("product_id", "category_name", "price", "views", "units", "upv", "cat_instock_median_upv",
           "lost_units", "lost_revenue").toPandas().to_csv(os.path.join(OUT, "instock_conversion_leakage.csv"), index=False)
by_cat.toPandas().to_csv(os.path.join(OUT, "leakage_by_category.csv"), index=False)

total_rev = p.agg(F.sum("revenue")).first()[0]
s_so = so.agg(F.count("*").alias("n"), F.sum("lost_revenue").alias("lost"), F.sum("lost_units").alias("lu")).first()
s_in = ins.agg(F.count("*").alias("n"), F.sum("lost_revenue").alias("lost")).first()
top50 = ins.orderBy(F.desc("lost_revenue")).limit(50).agg(F.sum("lost_revenue")).first()[0]
summary = {
    "total_revenue": round(total_rev, 2),
    "products_by_state": {r["state"]: r["products"] for r in evidence.collect()},
    "v1_naive_leakage_rejected": 125925052.27,
    "stockout_est_lost_units": round(s_so["lu"], 0),
    "stockout_est_lost_revenue": round(s_so["lost"], 2),
    "stockout_loss_pct_of_revenue": round(100 * s_so["lost"] / total_rev, 1),
    "instock_conversion_leakage": round(s_in["lost"], 2),
    "instock_leakage_pct_of_revenue": round(100 * s_in["lost"] / total_rev, 1),
    "instock_leakage_share_top50_products": round(top50 / s_in["lost"], 3),
    "views_on_inactive_products": int(p.filter("state='inactive'").agg(F.sum("views")).first()[0] or 0),
    "timings_s": {"hbase_scan": round(t_hbase, 2), "mongo_find": round(t_mongo, 3)},
}
print(json.dumps(summary, indent=2, default=str))
json.dump(summary, open(os.path.join(OUT, "leakage_summary.json"), "w"), indent=2, default=str)
spark.stop()
