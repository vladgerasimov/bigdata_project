from pyspark.sql import SparkSession
from SparkUpdateTables import update_df_prices_history
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName('yurkin_create_tables')\
                    .getOrCreate()

print('start ss')
links = 'hdfs:///user/andreyyur/project/df_link_vendor_code.parquet'
res_to_update = []
df = spark.read.parquet(links).toPandas()

print('start cycle')
for l in list(df.link):
    # print(l)

    page = requests.get(l)
    soup = BeautifulSoup(page.text, "html.parser")

    raw_price = soup.find('div', class_='fMDws sRt8j')
    # print('raw_priceraw_price', raw_price)
    price = int(raw_price.contents[0].strip().replace(' ', ''))

    raw_code = soup.find('div', class_='ul6Oh')
    code = int(raw_code.contents[0].strip())

    current_datetime = datetime.now()+timedelta(hours=3)

    res = tuple([code, price, current_datetime.strftime("%Y-%m-%d %H:%M:%S")])
    res_to_update.append(res)

# print(res_to_update)
print("got_result of parsing")
update_df_prices_history(res_to_update)