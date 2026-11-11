"""
Integrated query 1: Customer Lifetime Value, and whether browsing engagement adds anything.

BUSINESS QUESTION
  "Which customers will be worth the most next quarter? Does on-site engagement
   (sessions, time on site) predict future spend BEYOND what purchase history says?"
  If it doesn't, the business can drop engagement data from its CLV pipeline. That
  saves joining a 2M-session time series to the customer table every night.

DATA SOURCES
  MongoDB  transactions         purchase history (pushdown aggregation per user, split at cutoff)
  MongoDB  users                profile + registration date
  HBase    user_activity        engagement before the cutoff (family 's' only, server-side filter)
  Parquet  transaction_items +  category mix -> estimated margin per customer
           subcategories

METHOD (calibration / holdout)
  The 90-day window is split at CUTOFF = window start + 60 days.
  Features come only from the first 60 days. The target is spend in the last 30 days.
  Two Spark MLlib linear regressions are trained on the same users:
     M1 = purchase history only (frequency, monetary, recency, tenure)
     M2 = M1 + engagement from HBase (sessions, minutes, page views, converted sessions)
  Both are evaluated on held-out users (70/30 split). If M2 doesn't beat M1, engagement
  adds no predictive value.
  Projected 12-month CLV = predicted 30-day spend * 12 * estimated margin (no discounting:
  stated simplification).
"""
import json
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "spark"))
from common import CLEAN, OUT, spark_session                     # noqa: E402
from stores import MONGO_URI, hbase_user_engagement              # noqa: E402

from pymongo import MongoClient                                  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator            # noqa: E402
from pyspark.ml.feature import StandardScaler, VectorAssembler   # noqa: E402
from pyspark.ml.regression import LinearRegression               # noqa: E402
from pyspark.sql import functions as F                           # noqa: E402
from pyspark.sql import types as T                               # noqa: E402
from pyspark.sql.window import Window                            # noqa: E402

spark = spark_session("06-integrated-clv")
db = MongoClient(MONGO_URI).ecommerce
timings = {}

# ------------------------------------------------ define the window from the data itself
span = list(db.transactions.aggregate([{"$group": {"_id": None, "lo": {"$min": "$timestamp"},
                                                   "hi": {"$max": "$timestamp"}}}]))[0]
start, end = span["lo"], span["hi"]
cutoff = start + timedelta(days=60)
print(f"window {start} -> {end}; cutoff {cutoff}")

# ------------------------------------------------ MongoDB: calibration + holdout aggregates
t0 = time.time()
rows = list(db.transactions.aggregate([
    {"$group": {
        "_id": "$user_id",
        "cal_orders": {"$sum": {"$cond": [{"$lt": ["$timestamp", cutoff]}, 1, 0]}},
        "cal_spend": {"$sum": {"$cond": [{"$lt": ["$timestamp", cutoff]}, "$total", 0]}},
        "cal_last": {"$max": {"$cond": [{"$lt": ["$timestamp", cutoff]}, "$timestamp", None]}},
        "hold_spend": {"$sum": {"$cond": [{"$gte": ["$timestamp", cutoff]}, "$total", 0]}},
    }}], allowDiskUse=True))
timings["mongo_txn_agg_s"] = round(time.time() - t0, 2)
users = {u["_id"]: u["registration_date"] for u in db.users.find({}, {"registration_date": 1})}

recs = []
for r in rows:
    last = r["cal_last"]
    recs.append((r["_id"], int(r["cal_orders"]), float(r["cal_spend"]),
                 float((cutoff - last).days) if last else 60.0,           # recency (days before cutoff)
                 float((cutoff - users[r["_id"]]).days),                  # tenure at cutoff
                 float(r["hold_spend"])))
hist = spark.createDataFrame(recs, "user_id string, cal_orders int, cal_spend double, recency_days double, "
                                   "tenure_days double, hold_spend double")
all_users = spark.createDataFrame([(u, float((cutoff - d).days)) for u, d in users.items()],
                                  "user_id string, tenure_days_all double")
hist = (all_users.join(hist.drop("tenure_days"), "user_id", "left")
        .na.fill({"cal_orders": 0, "cal_spend": 0.0, "recency_days": 60.0, "hold_spend": 0.0})
        .withColumnRenamed("tenure_days_all", "tenure_days"))

# ------------------------------------------------ HBase: engagement before the cutoff
t0 = time.time()
eng = hbase_user_engagement(spark, before=cutoff.isoformat())
timings["hbase_engagement_scan_s"] = round(time.time() - t0, 2)

# ------------------------------------------------ Parquet: margin estimate per customer
margin = (spark.read.parquet(os.path.join(CLEAN, "subcategories"))
          .groupBy("category_id").agg(F.avg("profit_margin").alias("m")))
cust_margin = (spark.read.parquet(os.path.join(CLEAN, "transaction_items"))
               .join(spark.read.parquet(os.path.join(CLEAN, "products")).select("product_id", "category_id"),
                     "product_id")
               .join(F.broadcast(margin), "category_id")
               .groupBy("user_id")
               .agg((F.sum(F.col("subtotal") * F.col("m")) / F.sum("subtotal")).alias("est_margin")))

# ------------------------------------------------ Spark: join the three sources
df = (hist.join(eng, "user_id", "inner")          # inner: modelling set = HBase-sampled users
      .join(cust_margin, "user_id", "left")
      .withColumn("session_minutes", F.col("total_session_seconds") / 60.0)
      .na.fill({"est_margin": float(margin.agg(F.avg("m")).first()[0])})
      .cache())
n = df.count()
print(f"modelling set: {n} users (all users present in HBase sample)")

hist_feats = ["cal_orders", "cal_spend", "recency_days", "tenure_days"]
eng_feats = ["sessions", "session_minutes", "page_views", "converted_sessions"]
train, test = df.randomSplit([0.7, 0.3], seed=42)
results = {}
models = {}
for name, feats in [("M1_history", hist_feats), ("M2_history_plus_engagement", hist_feats + eng_feats)]:
    va = VectorAssembler(inputCols=feats, outputCol="raw")
    sc = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)
    tr = va.transform(train)
    scm = sc.fit(tr)
    lr = LinearRegression(featuresCol="features", labelCol="hold_spend", regParam=0.01)
    m = lr.fit(scm.transform(tr))
    pred = m.transform(scm.transform(va.transform(test)))
    ev = lambda metric: RegressionEvaluator(labelCol="hold_spend", metricName=metric).evaluate(pred)
    results[name] = {"test_r2": round(ev("r2"), 4), "test_mae": round(ev("mae"), 2),
                     "test_rmse": round(ev("rmse"), 2),
                     "coefficients(std)": dict(zip(feats, [round(c, 2) for c in m.coefficients]))}
    models[name] = (va, scm, m)

# naive baseline: next-30-day spend = calibration spend / 2 (60 days -> 30 days)
base = test.withColumn("p", F.col("cal_spend") / 2).select(
    F.avg(F.abs(F.col("p") - F.col("hold_spend"))).alias("mae")).first()["mae"]
results["baseline_half_of_60d_spend"] = {"test_mae": round(base, 2)}

# simple correlations on the full modelling set: interpretable for the report
corr = {f: round(df.stat.corr(f, "hold_spend"), 4) for f in hist_feats + eng_feats}

# ------------------------------------------------ CLV with the chosen model (M1 unless M2 is clearly better)
best = ("M2_history_plus_engagement"
        if results["M2_history_plus_engagement"]["test_mae"] < 0.97 * results["M1_history"]["test_mae"]
        else "M1_history")
va, scm, m = models[best]
clv = (m.transform(scm.transform(va.transform(df)))
       .withColumn("pred_30d_spend", F.greatest(F.col("prediction"), F.lit(0.0)))
       .withColumn("clv_12m_est", F.round(F.col("pred_30d_spend") * 12 * F.col("est_margin"), 2))
       .select("user_id", "cal_orders", "cal_spend", "recency_days", "sessions", "session_minutes",
               "est_margin", "hold_spend", F.round("pred_30d_spend", 2).alias("pred_30d_spend"), "clv_12m_est"))
clv = clv.withColumn("clv_decile", F.ntile(10).over(Window.orderBy(F.desc("clv_12m_est"))))
clv.toPandas().to_csv(os.path.join(OUT, "clv_by_user.csv"), index=False)
dec = (clv.groupBy("clv_decile").agg(F.count("*").alias("users"),
                                     F.round(F.avg("clv_12m_est"), 2).alias("avg_clv"),
                                     F.round(F.sum("hold_spend"), 2).alias("actual_holdout_spend"))
       .orderBy("clv_decile"))
dec.show()
dec.toPandas().to_csv(os.path.join(OUT, "clv_deciles.csv"), index=False)
top_share = clv.filter("clv_decile = 1").agg(F.sum("hold_spend")).first()[0] / \
    clv.agg(F.sum("hold_spend")).first()[0]

summary = {"window_start": str(start), "window_end": str(end), "cutoff": str(cutoff),
           "modelling_users": n, "model_selected_for_clv": best, "models": results,
           "pearson_corr_with_holdout_spend": corr,
           "top_decile_share_of_actual_holdout_spend": round(top_share, 4), "timings": timings}
print(json.dumps(summary, indent=2))
json.dump(summary, open(os.path.join(OUT, "clv_summary.json"), "w"), indent=2)
spark.stop()
