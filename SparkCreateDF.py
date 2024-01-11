from pyspark.sql import SparkSession

# Create a Spark session

spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()


# Create a sample DataFrame (replace this with your actual DataFrame)
df_user_vendor_code = [(270131364, 19000213474)]
columns = ["user_id", "vendor_code"]
df_user_vendor_code = spark.createDataFrame(df_user_vendor_code, columns)
hdfs_path = "hdfs:///user/andreyyur/project/df_user_vendor_code.parquet"
df_user_vendor_code.write.parquet(hdfs_path, mode="overwrite")


df_link_vendor_code = [('https://goldapple.ru/19000213474-calendrier-de-l-avent', 
                        19000213474,
                        'PAYOT Calendrier de l’avent')]
columns = ["link", "vendor_code", "goods_name"]
df_link_vendor_code = spark.createDataFrame(df_link_vendor_code, columns)
hdfs_path = "hdfs:///user/andreyyur/project/df_link_vendor_code.parquet"
df_link_vendor_code.write.parquet(hdfs_path, mode="overwrite")


df_prices_history = [(19000213474,
                      6240,
                      '2024-01-09 22:44:54')]
columns = ["vendor_code", "price", "datetime"]
df_prices_history = spark.createDataFrame(df_prices_history, columns)
hdfs_path = "hdfs:///user/andreyyur/project/df_prices_history.parquet"
df_prices_history.write.parquet(hdfs_path, mode="overwrite")

# Stop the Spark session
spark.stop()