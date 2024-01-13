from pyspark.sql import SparkSession

spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()


# hdfs_path = "hdfs:///user/andreyyur/project/df_prices_history.parquet"
print("spark жив")
hdfs_path = "hdfs:///user/andreyyur/project/df_link_vendor_code.parquet"
# hdfs_path = "hdfs:///user/andreyyur/project/df_user_vendor_code.parquet"


df = spark.read.parquet(hdfs_path).toPandas()
print(df)
