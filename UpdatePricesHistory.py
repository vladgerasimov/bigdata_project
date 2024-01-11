from pyspark.sql import SparkSession
from PageInfo import get_page_info
from SparkUpdateTables import update_df_prices_history
# from airflow import DAG 
# import pendulum
# from datetime import timedelta


def update_and_parse_prices(links):
    spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName('yurkin_create_tables')\
                    .getOrCreate()

    # hdfs_path = "hdfs:///user/andreyyur/project/df_link_vendor_code.parquet"
    df = spark.read.parquet(links).toPandas()

    for l in list(df.link):
        res = (get_page_info(l))
        res = [res[0], res[2], res[3]]

        update_df_prices_history(res)
    

    spark.stop()

