from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    prices_history_table: str = "hdfs:///user/andreyyur/project/df_prices_history.parquet"
    link_vendor_code_table: str = "hdfs:///user/andreyyur/project/df_link_vendor_code.parquet"
    user_vendor_code_table: str = "hdfs:///user/andreyyur/project/df_user_vendor_code.parquet"
    api_url: str = "https://api.telegram.org/bot6560671331:AAEuzxZolY_uOOM0ypG0VuoOrtMfozBVJfE/sendMessage"


app_settings = AppSettings()
