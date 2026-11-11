"""
Spark SQL analytics.

Part A: complex SQL over the cleaned Parquet tables (temp views).
Part B: SQL over data that lives in the operational stores. MongoDB users (with their
        embedded purchase_summary) and HBase product_daily are pulled into DataFrames,
        registered as views, and joined in one SQL statement.

Each query's result is printed and saved to output/sql_<name>.csv.
"""
import os
import sys

from common import CLEAN, OUT, spark_session

spark = spark_session("05-spark-sql")
for t in ["users", "products", "categories", "subcategories", "price_history", "transactions",
          "transaction_items", "sessions", "page_views", "cart_items"]:
    spark.read.parquet(os.path.join(CLEAN, t)).createOrReplaceTempView(t)


def run(name, sql, show=20):
    print(f"\n===== {name} =====")
    df = spark.sql(sql)
    df.show(show, truncate=False)
    df.toPandas().to_csv(os.path.join(OUT, f"sql_{name}.csv"), index=False)
    return df


# ---------------------------------------------------------------- A1: funnel by device x referrer
run("funnel_device_referrer", """
WITH s AS (
  SELECT device_type, referrer,
         COUNT(*)                                      AS sessions,
         SUM(CAST(reached_product AS INT))             AS product_view,
         SUM(CAST(n_cart_lines > 0 AS INT))            AS added_to_cart,
         SUM(CAST(reached_checkout AS INT))            AS checkout,
         SUM(CAST(conversion_status = 'converted' AS INT)) AS converted
  FROM sessions GROUP BY device_type, referrer)
SELECT *, ROUND(100.0 * converted / sessions, 2)        AS conv_rate_pct,
          ROUND(100.0 * converted / NULLIF(added_to_cart, 0), 1) AS cart_to_conv_pct
FROM s ORDER BY conv_rate_pct DESC
""", show=15)

# ---------------------------------------------------------------- A2: best product per category (window)
run("top_product_per_category", """
WITH rev AS (
  SELECT p.category_id, ti.product_id, p.name,
         SUM(ti.quantity) AS units, ROUND(SUM(ti.subtotal), 2) AS revenue
  FROM transaction_items ti JOIN products p USING (product_id)
  GROUP BY p.category_id, ti.product_id, p.name),
views AS (
  SELECT product_id, COUNT(*) AS views FROM page_views
  WHERE page_type = 'product_detail' GROUP BY product_id),
ranked AS (
  SELECT r.*, v.views, ROUND(r.units / v.views, 4) AS units_per_view,
         RANK() OVER (PARTITION BY r.category_id ORDER BY r.revenue DESC) AS rnk,
         ROUND(100 * r.revenue / SUM(r.revenue) OVER (PARTITION BY r.category_id), 2) AS pct_of_category
  FROM rev r LEFT JOIN views v USING (product_id))
SELECT c.category_name, ranked.* FROM ranked JOIN categories c USING (category_id)
WHERE rnk = 1 ORDER BY revenue DESC
""", show=25)

# ---------------------------------------------------------------- A3: price veracity check
# Was each line sold at the price that was valid at the time of the sale?
# Non-equi join + window: the latest price_history row with valid_from <= sale time.
run("price_at_time_of_sale", """
WITH candidates AS (
  SELECT ti.transaction_id, ti.product_id, ti.ts, ti.unit_price, ph.price AS list_price, ph.valid_from,
         ROW_NUMBER() OVER (PARTITION BY ti.transaction_id, ti.line_no ORDER BY ph.valid_from DESC) AS rn
  FROM transaction_items ti JOIN price_history ph
    ON ti.product_id = ph.product_id AND ph.valid_from <= ti.ts),
matched AS (SELECT * FROM candidates WHERE rn = 1)
SELECT
  COUNT(*)                                                         AS lines_checked,
  SUM(CAST(ABS(unit_price - list_price) < 0.005 AS INT))           AS sold_at_valid_price,
  SUM(CAST(ABS(unit_price - list_price) >= 0.005 AS INT))          AS sold_at_other_price,
  ROUND(100.0 * SUM(CAST(ABS(unit_price - list_price) >= 0.005 AS INT)) / COUNT(*), 2) AS pct_mismatch,
  ROUND(AVG(unit_price - list_price), 3)                           AS avg_diff,
  ROUND(SUM(unit_price - list_price), 2)                           AS revenue_effect
FROM matched
""")

# ---------------------------------------------------------------- A4: inter-purchase interval (LAG)
run("repeat_purchase_interval", """
WITH t AS (
  SELECT user_id, ts,
         (unix_timestamp(ts) - unix_timestamp(LAG(ts) OVER (PARTITION BY user_id ORDER BY ts))) / 86400.0 AS gap_days
  FROM transactions)
SELECT COUNT(gap_days) AS repeat_orders,
       ROUND(percentile_approx(gap_days, 0.25), 2) AS p25_days,
       ROUND(percentile_approx(gap_days, 0.5), 2)  AS median_days,
       ROUND(percentile_approx(gap_days, 0.75), 2) AS p75_days,
       ROUND(AVG(gap_days), 2) AS mean_days
FROM t
""")

# ---------------------------------------------------------------- A5: revenue by category and week, with margin estimate
run("category_weekly_revenue", """
WITH m AS (SELECT category_id, AVG(profit_margin) AS est_margin FROM subcategories GROUP BY category_id)
SELECT c.category_name, date_trunc('week', ti.ts) AS week,
       ROUND(SUM(ti.subtotal), 2) AS gross_revenue,
       ROUND(SUM(ti.subtotal) * MAX(m.est_margin), 2) AS est_gross_profit
FROM transaction_items ti
JOIN products p USING (product_id) JOIN categories c USING (category_id) JOIN m USING (category_id)
GROUP BY c.category_name, date_trunc('week', ti.ts)
ORDER BY week, gross_revenue DESC
""", show=10)

# ---------------------------------------------------------------- Part B: SQL across MongoDB + HBase
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stores import hbase_product_daily, mongo_users_with_summary   # noqa: E402

mongo_users_with_summary(spark).createOrReplaceTempView("mongo_users")
hbase_product_daily(spark).createOrReplaceTempView("hbase_product_daily")

run("cross_store_country_value", """
-- Which countries hold the most valuable customers, and what do their buyers' favourite
-- products look like in the HBase daily performance series?
WITH buyers AS (SELECT * FROM mongo_users WHERE orders > 0),
fav AS (  -- each country's most purchased product (from the Parquet line items)
  SELECT u.country, ti.product_id, SUM(ti.quantity) AS units,
         ROW_NUMBER() OVER (PARTITION BY u.country ORDER BY SUM(ti.quantity) DESC) AS rn
  FROM transaction_items ti JOIN mongo_users u USING (user_id)
  GROUP BY u.country, ti.product_id),
perf AS (  -- that product's lifetime funnel from HBase
  SELECT product_id, SUM(views) AS views, SUM(units_sold) AS units_sold,
         ROUND(SUM(units_sold) / NULLIF(SUM(views), 0), 4) AS units_per_view
  FROM hbase_product_daily GROUP BY product_id)
SELECT b.country, COUNT(*) AS buyers, ROUND(AVG(b.total_spent), 2) AS avg_spent,
       ROUND(SUM(b.total_spent), 2) AS revenue,
       MAX(f.product_id) AS top_product, MAX(p.views) AS top_product_views,
       MAX(p.units_per_view) AS top_product_units_per_view
FROM buyers b
LEFT JOIN fav f  ON f.country = b.country AND f.rn = 1
LEFT JOIN perf p ON p.product_id = f.product_id
GROUP BY b.country ORDER BY revenue DESC
""", show=10)

spark.stop()
