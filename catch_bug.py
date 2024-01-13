from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def get_page_info(url:str):
    # url = 'https://goldapple.ru/65970100003-hydrating-essence'
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")
    raw_price = soup.find('div', itemprop='offers')
    price = int(raw_price.contents[0].contents[0].strip().replace(' ', ''))

    raw_goods_name = soup.find('div', value='Description_0')
    goods_name = raw_goods_name.contents[0].contents[0].contents[0].contents[0].contents[0].text.strip()

    raw_code = soup.find('div', value='Description_0')
    code = raw_code.contents[0].contents[0].contents[0].contents[0].contents[2].text.strip()

    current_datetime = datetime.now()+timedelta(hours=3)

    return tuple([int(code), goods_name.replace("'", "`"), price, current_datetime.strftime("%Y-%m-%d %H:%M:%S")])


def update_df_prices_history(data_to_update, spark):
        
        # columns = ["vendor_code", "price", "datetime"]
        schema = StructType([
                        StructField("vendor_code", LongType(), True),
                        StructField("price", LongType(), True),
                        StructField("datetime", StringType(), True)
                        ])

        rdd = spark.sparkContext.parallelize(data_to_update)
        df = spark.createDataFrame(rdd, schema)

        df.write.mode('append').parquet("hdfs:///user/andreyyur/project/df_prices_history.parquet")



def update_and_parse_prices(links):
    print('update_and_parse_prices')
    spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName('yurkin_create_tables')\
                    .getOrCreate()

    # hdfs_path = "hdfs:///user/andreyyur/project/df_link_vendor_code.parquet"
    df = spark.read.parquet(links).toPandas()
    res_to_update = []

    for l in list(df.link):
        try:
            res = (get_page_info(l))
        except:
            res = (0, '0', 0, '0')
        res = [res[0], res[2], res[3]]

        res_to_update.append(res)

    update_df_prices_history(res_to_update, spark)

    spark.stop()

update_and_parse_prices("hdfs:///user/andreyyur/project/df_link_vendor_code.parquet")