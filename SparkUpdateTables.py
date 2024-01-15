import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

from core.settings import app_settings



def update_df_link_vendor_code(data_to_update, spark):
        schema_df_link_vendor_code = StructType([
            StructField("link", StringType(), True),
            StructField("vendor_code", LongType(), True),
            StructField("goods_name", StringType(), True)
        ])
        df = spark.createDataFrame([data_to_update], schema=schema_df_link_vendor_code)

        df.write.mode('append').parquet(app_settings.link_vendor_code_table)


def update_df_user_vendor_code(data_to_update, spark):
        schema_df_user_vendor_code = StructType([
            StructField("user_id", LongType(), True),
            StructField("vendor_code", LongType(), True),
            StructField("discount_percent", IntegerType(), True)
        ])
        df = spark.createDataFrame([data_to_update], schema=schema_df_user_vendor_code)
        print("------> df created")
        df.write.mode('append').parquet(app_settings.user_vendor_code_table)


def check_df_user_vendor_code(data_to_check, spark) -> int:
        existing_df = spark.read.parquet("hdfs:///user/andreyyur/project/df_user_vendor_code.parquet")
        user_id, vendor_code, discount_percent, *_ = data_to_check
        if discount_percent:
            discount_condition = F.col("discount_percent") == F.lit(discount_percent)
        else:
            discount_condition = F.col("discount_percent").isNull() | F.isnan(F.col("discount_percent"))

        rows_count = (
            existing_df
            .filter(
                (F.col("user_id") == F.lit(user_id))
                & (F.col("vendor_code") == F.lit(vendor_code))
                & discount_condition
            ).count()
        )

        return rows_count

def check_df_link_vendor_code(data_to_check, spark):
        existing_df = spark.read.parquet(app_settings.link_vendor_code_table)
        link, vendor_code, goods_name = data_to_check

        rows_count = (
            existing_df
            .filter(
                (F.col("link") == F.lit(link))
                & (F.col("vendor_code") == F.lit(vendor_code))
                & (F.col("goods_name") == F.lit(goods_name))
            ).count()
        )

        return rows_count


def update_df_prices_history(data_to_update, spark):
        schema = StructType([
            StructField("vendor_code", LongType(), True),
            StructField("price", LongType(), True),
            StructField("datetime", StringType(), True)
        ])

        df = spark.createDataFrame([data_to_update], schema=schema)

        df.write.mode('append').parquet(app_settings.prices_history_table)
