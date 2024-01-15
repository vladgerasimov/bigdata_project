from dataclasses import dataclass

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from analytics.prices import get_price_changes
from core.settings import app_settings


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

        df.write.mode("append").parquet(app_settings.prices_history_table)


@dataclass
class ItemData:
    goods_name: str
    link: str

    def __str__(self):
        return f"[{self.goods_name}]({self.link})"


def get_user_followed_items(grouped_df: pd.DataFrame):
    return [ItemData(**row) for row in grouped_df.to_dict(orient="records")]


def get_items_for_users(spark) -> dict[int, list[str]]:
    price_changes = get_price_changes(spark)
    users = spark.read.parquet(app_settings.user_vendor_code_table)
    items = spark.read.parquet(app_settings.link_vendor_code_table)
    df = price_changes.join(
        users,
        on="vendor_code",
        how="inner"
    ).join(
        items,
        on="vendor_code",
        how="inner"
    ).filter(
        F.col("price_diff_percent") <= -F.col("discount_percent")
    )

    df = df.select("user_id", "goods_name", "link").toPandas()
    grouped_dfs = df.groupby("user_id")[["goods_name", "link"]]
    result = {user: get_user_followed_items(grouped_df) for user, grouped_df in grouped_dfs}
    return result



def notify_user(user_id, items: list[str]):
    items_message = '\n•'.join(map(str, items))
    message = f"Пора за покупками\!\nПроизошло снижение цен на интересующие вас товары:\n•{items_message}"

    params = {
        'chat_id': user_id,
        'text': message,
        'parse_mode': "MarkdownV2"
    }
    requests.post(app_settings.api_url, params=params)


def update_and_parse_prices(links):
    print('update_and_parse_prices')
    spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName('yurkin_create_tables')\
                    .getOrCreate()

    df = spark.read.parquet(links).toPandas()
    res_to_update = []

    for l in list(df.link):
        try:
            res = get_page_info(l)
        except:
            continue
        res = [res[0], res[2], res[3]]

        res_to_update.append(res)

    update_df_prices_history(res_to_update, spark)

    users_to_notify = get_items_for_users(spark)
    print(f"{users_to_notify=}")
    for user_id, items in users_to_notify.items():
        notify_user(user_id, items)

    spark.stop()

update_and_parse_prices(app_settings.link_vendor_code_table)
