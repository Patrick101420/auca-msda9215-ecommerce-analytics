# Distributed Multi-Model Analytics for E-commerce Data

AUCA MSDA9215 Big Data Analytics, final project. MongoDB (document), HBase (wide-column) and
Apache Spark (distributed processing) on a synthetic 90-day e-commerce dataset:
10k users, 5k products, 2M sessions (20M page views), 500k transactions.

## Architecture

```
dataset_generator.py ──► data/raw/*.json (4.0 GB)
                              │
          ┌───────────────────┼──────────────────────────┐
          ▼                   ▼                          ▼
   MongoDB (operational)   Spark batch (analytical)   HBase (time-series serving)
   catalogue, users +      01 clean → Parquet 545 MB   user_activity  user#revts#session
   purchase_summary,       02 product×day metrics ───► product_daily  product#yyyymmdd
   transactions, journeys  03 recommendations
          │                04 cohorts
          │                05 Spark SQL
          └──────────► Spark (integration) ◄───────────────┘
                     06 CLV (Mongo + HBase + Parquet, MLlib)
                     07 stock-out / leakage (HBase + Mongo)
                     benchmark: same query on all three engines
```

## Layout

| Path | What |
|---|---|
| `src/dataset_generator.py` | course generator, adapted to stream sessions to disk (diff vs `dataset_generator_original.py`) |
| `docker-compose.yml` | MongoDB 7.0 + HBase 2.1 (Thrift on 9090) |
| `mongodb/load_mongo.py` | document model, bulk load, indexes, computed `purchase_summary` via `$merge` |
| `mongodb/aggregations.py` | P1 revenue by category/month + top-3, P2 spend quintiles (`$bucketAuto`), P3 viewed vs sold, `explain()` |
| `hbase/schema.hbase` | table + column-family design with pre-split regions |
| `hbase/load_hbase.py` | Thrift loader (sessions sampled **by user**; product_daily from Spark) |
| `hbase/queries.py`, `hbase/queries.hbase` | prefix, range, value, hybrid and product time-range scans (Python + shell) |
| `spark/01..05_*.py` | cleaning, product metrics, recommendations + shuffle test, cohorts, Spark SQL |
| `spark/stores.py` | read MongoDB / HBase into Spark DataFrames with push-down |
| `integration/06_integrated_clv.py` | CLV with calibration/holdout, MLlib, Mongo + HBase + Parquet |
| `integration/07_demand_leakage.py` | stock-out vs conversion losses, HBase + Mongo |
| `integration/benchmark_engines.py` | same two questions on MongoDB, HBase and Spark |
| `viz/make_figures.py` → `viz/figures/` | report figures |
| `report/final_report.pdf` | technical report (AUCA format); rebuild with `python report/build_report.py` |
| `output/` | every result table/JSON the report quotes |
| `logs/` | console output of the actual runs |

## Reproduce

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run_all.sh
```

Notes for this machine: the Hadoop config sets `fs.defaultFS=hdfs://localhost:9000`, so `spark/common.py`
pins `spark.hadoop.fs.defaultFS=file:///`. The HBase image is amd64-only (emulated on Apple Silicon),
so HBase timings are pessimistic.

## Headline results (from `output/`)

| | Result |
|---|---|
| Data quality | 2 session-ID collisions (truncated UUIDs); 149,276 "converted" sessions with no transaction; 27.9% of sold lines priced differently from the list price valid at sale time; 48.7% of orders (57.6% of revenue) not linked to any session |
| Engines | user's last 5 sessions: MongoDB 0.3 ms, HBase 3.5 ms, Spark 138 ms; top-20 products / 30 days: Spark 86 ms, HBase 1.9 s, MongoDB 17.5 s |
| Recommendations | co-view lift distribution identical to shuffled baskets (median 1.43 vs 1.43): no real affinity |
| Cohorts | 0.55 orders/member/day in every cohort (±6%); no cohort effect |
| CLV | holdout R² ≈ 0 with and without HBase engagement; future spend not predictable in this data |
| Demand | 2,492 / 5,000 products sold out and lost 73% of their views; stock-outs ≈ $252M lost (upper bound) vs $7.4M from weak conversion |
