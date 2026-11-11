#!/usr/bin/env bash
# Reproduce the whole project end-to-end (~15 min on a 15-core / 24 GB laptop).
# Prereqs: Docker running, Java 17, Python 3.12 venv with requirements.txt installed in .venv
set -euo pipefail
cd "$(dirname "$0")"
PY=.venv/bin/python
mkdir -p logs

echo "== 0. infrastructure"
docker compose up -d
until docker exec ecom-hbase sh -c 'ps aux | grep -q [T]hriftServer'; do sleep 3; done
docker exec -i ecom-hbase hbase shell -n < hbase/schema.hbase > logs/hbase_schema.log 2>&1 || true

echo "== 1. generate dataset (2M sessions, 500k transactions)"
[ -f data/raw/sessions_19.json ] || $PY src/dataset_generator.py data/raw | tee logs/generator.log

echo "== 2. MongoDB load + aggregations"
$PY mongodb/load_mongo.py data/raw | tee logs/mongo_load.log
$PY mongodb/aggregations.py | tee logs/mongo_aggregations.log

echo "== 3. Spark batch jobs"
(cd spark && ../$PY 01_clean_normalize.py && ../$PY 02_product_daily.py \
            && ../$PY 03_recommendations.py && ../$PY 04_cohort_analysis.py) 2>&1 | tee logs/spark_batch.log

echo "== 4. HBase load + queries"
$PY hbase/load_hbase.py sessions data/raw --user-mod 10 | tee logs/hbase_sessions.log
$PY hbase/load_hbase.py product_daily output/product_daily_csv --batch-size 5000 | tee logs/hbase_product_daily.log
$PY hbase/queries.py user_000420 prod_00640 | tee logs/hbase_queries.log

echo "== 5. Spark SQL (incl. cross-store Mongo+HBase query)"
(cd spark && ../$PY 05_spark_sql.py) 2>&1 | tee logs/spark_05_sql.log

echo "== 6. integrated analytics + benchmark"
$PY integration/06_integrated_clv.py 2>&1 | tee logs/integration_06_clv.log
$PY integration/07_demand_leakage.py 2>&1 | tee logs/integration_07_leakage.log
$PY integration/benchmark_engines.py 2>&1 | tee logs/benchmark.log

echo "== 7. figures"
$PY viz/make_figures.py
echo "done: see output/ and viz/figures/"
