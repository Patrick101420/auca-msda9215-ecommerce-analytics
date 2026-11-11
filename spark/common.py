"""Shared Spark session + paths for all jobs."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "data", "raw")
CLEAN = os.path.join(ROOT, "data", "clean")
OUT = os.path.join(ROOT, "output")

# Spark starts Python workers as separate processes. Without this they can pick up the
# macOS system python3 (3.9), which cannot load this PySpark build.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def spark_session(app):
    from pyspark.sql import SparkSession
    return (SparkSession.builder
            .master("local[12]")                      # 12 of 15 cores; leave room for Docker
            .appName(app)
            .config("spark.driver.memory", "10g")     # local mode: driver == executor
            .config("spark.sql.shuffle.partitions", "48")  # default 200 is too many for ~GBs locally
            .config("spark.sql.adaptive.enabled", "true")  # AQE coalesces small shuffle partitions
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.showConsoleProgress", "false")
            # The machine's Hadoop config sets fs.defaultFS=hdfs://localhost:9000. This project
            # runs on local disk, so pin the default FS (otherwise: "RPC response has invalid length").
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.eventLog.enabled", "false")
            .getOrCreate())


def p(*parts):
    """Absolute local path as a file:// URI (the project path contains spaces)."""
    return "file://" + os.path.join(*parts)
