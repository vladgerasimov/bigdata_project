from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType



def update_df_link_vendor_code(data_to_update):
        spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()
        
        columns = ["link", "vendor_code", "goods_name"]
        data = [data_to_update]

        df = spark.createDataFrame(data, columns)

        df.write.mode('append').parquet("hdfs:///user/andreyyur/project/df_link_vendor_code.parquet")

        spark.stop()


def update_df_user_vendor_code(data_to_update):
        spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()
        
        columns = ["user_id", "vendor_code"]
        data = [data_to_update]

        df = spark.createDataFrame(data, columns)

        df.write.mode('append').parquet("hdfs:///user/andreyyur/project/df_user_vendor_code.parquet")

        spark.stop()


def check_df_user_vendor_code(data_to_check):
        spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()

        existing_df = spark.read.parquet("hdfs:///user/andreyyur/project/df_user_vendor_code.parquet")
        existing_df.createOrReplaceTempView("existing_df_view")

        query = f"""
                SELECT COUNT(*) as count_check
                FROM existing_df_view
                WHERE user_id = {data_to_check[0]}
                AND vendor_code = {data_to_check[1]}
                """
        count_df = spark.sql(query)

        count_value = count_df.first().asDict()["count_check"]

        spark.stop()

        return count_value


def check_df_link_vendor_code(data_to_check):
        spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()

        existing_df = spark.read.parquet("hdfs:///user/andreyyur/project/df_link_vendor_code.parquet")
        existing_df.createOrReplaceTempView("existing_df_view")

        query = f"""
                SELECT COUNT(*) as count_check
                FROM existing_df_view
                WHERE link = '{data_to_check[0]}'
                AND vendor_code = {data_to_check[1]}
                AND goods_name = '{data_to_check[2]}'
                """
        count_df = spark.sql(query)

        count_value = count_df.first().asDict()["count_check"]

        spark.stop()

        return count_value


def update_df_prices_history(data_to_update):
        spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_create_tables')\
                .getOrCreate()
        
        # columns = ["vendor_code", "price", "datetime"]
        schema = StructType([
                        StructField("vendor_code", LongType(), True),
                        StructField("price", LongType(), True),
                        StructField("datetime", StringType(), True)
                        ])

        rdd = spark.sparkContext.parallelize(data_to_update)
        df = spark.createDataFrame(rdd, schema)

        df.write.mode('append').parquet("hdfs:///user/andreyyur/project/df_prices_history.parquet")

        spark.stop()


# data = ('https://goldapple.ru/19000162062-essentiels-week-end-visage-corps',
#         19000162062,
#         'PAYOT Essentiels week-end visage&corps')

# print("Answer = > ", check_df_link_vendor_code(data_to_check = data))