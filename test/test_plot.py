from analytics.prices import save_prices_plot, get_price_history

from pyspark.sql import SparkSession

spark = spark = SparkSession.builder\
                .master("local[*]")\
                .appName('gerasimov_test_plot')\
                .getOrCreate()


vendor_code = 19000213474
prices_table = "hdfs:///user/andreyyur/project/df_prices_history.parquet"
price_history = get_price_history(prices_table, vendor_code, spark)
print(price_history)

plot_img = save_prices_plot(price_history)
print(plot_img)
