from PageInfo import get_page_info
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

link = 'https://goldapple.ru/19000215146-jardin-enchante'
info_link = get_page_info(link)

vendor_code = int(info_link[0])
goods_name = str(info_link[1])
user_id = 270131364

user_vendor_code_update = [[user_id, vendor_code, 1]]  # 1 нужно будет заменить на число от пользователя

spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()

print("------> spark alive")
schema_df_user_vendor_code = StructType([
    StructField("user_id", LongType(), True),
    StructField("vendor_code", LongType(), True),
    StructField("discount_percent", IntegerType(), True)
])

df = spark.createDataFrame(user_vendor_code_update, schema=schema_df_user_vendor_code)

print("------> df created")
df.write.mode('append').parquet("hdfs:///user/andreyyur/project/df_user_vendor_code.parquet")

spark.stop()
