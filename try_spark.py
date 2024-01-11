from pyspark.sql import SparkSession

# Create a Spark session

spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()

# import pandas as pd

hdfs_path = "hdfs:///user/andreyyur/project/df_prices_history.parquet"

df = spark.read.parquet(hdfs_path)
# df = pd.read_parquet(hdfs_path)

# print(12345678)