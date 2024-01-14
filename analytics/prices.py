from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

plots_dir = Path(__file__).parent.parent / "plots"
if not plots_dir.exists():
    plots_dir.mkdir()


def get_price_changes(hdfs_path: str, spark: SparkSession):
    df = spark.read.parquet(hdfs_path)
    lag_window = Window().partitionBy("vendor_code").orderBy("datetime")
    df = df.withColumn(
        "price_diff_percent",
        (F.lag(F.col("price"), 1).over(lag_window) / F.col("price") - 1) * 100
    )
    return df.withColumn(
        "last_datetime",
        F.last("datetime").over(lag_window)
    ).filter(
        F.col("datetime") == F.col("last_datetime")
    ).select("vendor_code", "price_diff_percent")


def get_price_history(hdfs_path: str, vendor_code: int, spark: SparkSession) -> dict[str, int]:
    df = spark.read.parquet(hdfs_path)
    prices = df.filter(
        F.col("vendor_code") == F.lit(vendor_code)
    ).orderBy("datetime").select("datetime", "price")
    return {row.datetime: row.price for row in prices.collect()}


def save_prices_plot(prices_history: dict[str, int]) -> Path:
    datetimes = prices_history.keys()
    prices = prices_history.values()

    plt.plot(datetimes, prices)
    plt.xlabel("datetime")
    plt.ylabel("price, rubles")
    file_name = plots_dir / f"prices_plot_{datetime.now()}"
    plt.save(file_name)
    return file_name
