from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType


spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()


schema = StructType([
                        StructField("vendor_code", LongType(), True),
                        StructField("price", LongType(), True),
                        StructField("datetime", StringType(), True)
                        ])
df_prices_history = [(19000213474,
                      6240,
                      '2024-01-09 22:44:54')]
df_prices_history = spark.createDataFrame(df_prices_history, schema)
hdfs_path = "hdfs:///user/andreyyur/project/df_prices_history.parquet"
df_prices_history.write.parquet(hdfs_path, mode="overwrite")


schema_df_link_vendor_code = StructType([
                                        StructField("link", StringType(), True),
                                        StructField("vendor_code", LongType(), True),
                                        StructField("goods_name", StringType(), True)
                                        ])
df_link_vendor_code = [('https://goldapple.ru/19000213474-calendrier-de-l-avent', 
                        19000213474,
                        'PAYOT Calendrier de l’avent')]
df_link_vendor_code = spark.createDataFrame(df_link_vendor_code, schema_df_link_vendor_code)
hdfs_path = "hdfs:///user/andreyyur/project/df_link_vendor_code.parquet"
df_link_vendor_code.write.parquet(hdfs_path, mode="overwrite")


schema_df_user_vendor_code = StructType([
                                        StructField("user_id", LongType(), True),
                                        StructField("vendor_code", LongType(), True),
                                        StructField("discount_percent", IntegerType(), True)
                                        ])
df_user_vendor_code = [(270131364, 19000213474, 10)]
df_user_vendor_code = spark.createDataFrame(df_user_vendor_code, schema_df_user_vendor_code)
hdfs_path = "hdfs:///user/andreyyur/project/df_user_vendor_code.parquet"
df_user_vendor_code.write.parquet(hdfs_path, mode="overwrite")

spark.stop()