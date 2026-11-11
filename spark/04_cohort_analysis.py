"""
Spark batch job 4: cohort analysis of purchasing behaviour.

Cohort = registration month. For each cohort and each calendar month of activity we compute
active buyers, retention (% of the cohort buying), revenue, and revenue per cohort member.

Note: the generator registers users 90-270 days before "now" but only simulates the last
90 days of activity. Every cohort is therefore observed only from the end of its "age 3
months" onwards, and younger cohorts are observed at younger ages. We index by
months-since-registration so cohorts are compared at the same age, and we say plainly that
the first months of life are unobserved.
"""
import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import CLEAN, OUT, spark_session

spark = spark_session("04-cohort")
rd = lambda n: spark.read.parquet(os.path.join(CLEAN, n))

users = rd("users").select("user_id", F.trunc("registration_date", "month").alias("cohort"))
cohort_size = users.groupBy("cohort").agg(F.count("*").alias("cohort_users"))

tx = (rd("transactions").filter(F.col("status") != "cancelled")
      .select("user_id", "total", F.trunc("ts", "month").alias("activity_month")))

cohort = (tx.join(users, "user_id")
          .groupBy("cohort", "activity_month")
          .agg(F.countDistinct("user_id").alias("buyers"),
               F.count("*").alias("orders"),
               F.round(F.sum("total"), 2).alias("revenue"))
          .join(cohort_size, "cohort")
          .withColumn("age_months", F.months_between("activity_month", "cohort").cast("int"))
          .withColumn("retention_pct", F.round(100 * F.col("buyers") / F.col("cohort_users"), 1))
          .withColumn("revenue_per_member", F.round(F.col("revenue") / F.col("cohort_users"), 2))
          .withColumn("aov", F.round(F.col("revenue") / F.col("orders"), 2))
          .orderBy("cohort", "activity_month"))

# Partial months: the 90-day window starts and ends mid-month, so raw monthly revenue
# swings ~+/-65% purely from the number of days observed. Normalise by observed days.
span = rd("transactions").agg(F.min("ts").alias("lo"), F.max("ts").alias("hi")).first()
lo, hi = F.lit(span["lo"]).cast("date"), F.lit(span["hi"]).cast("date")
month_end = F.last_day("activity_month")
cohort = (cohort
          .withColumn("days_observed", F.datediff(F.least(month_end, hi), F.greatest(F.col("activity_month"), lo)) + 1)
          .withColumn("rev_per_member_per_day",
                      F.round(F.col("revenue_per_member") / F.col("days_observed"), 2))
          .withColumn("orders_per_member_per_day",
                      F.round(F.col("orders") / F.col("cohort_users") / F.col("days_observed"), 4)))

# The window over each cohort's own months shows whether daily spend per member changes with age
w = Window.partitionBy("cohort").orderBy("activity_month")
cohort = cohort.withColumn("daily_rev_change_pct",
                           F.round(100 * (F.col("rev_per_member_per_day") /
                                          F.lag("rev_per_member_per_day").over(w) - 1), 1))

cohort.show(60, truncate=False)
cohort.toPandas().to_csv(os.path.join(OUT, "cohort_matrix.csv"), index=False)

# Partial months (the window starts and ends mid-month) distort the edges. Report the span.
span = rd("transactions").agg(F.min("ts").alias("first"), F.max("ts").alias("last")).first()
print("transaction window:", span["first"], "->", span["last"])
spark.stop()
