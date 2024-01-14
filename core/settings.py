from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    prices_history_table: str = "hdfs:///user/andreyyur/project/df_prices_history.parquet"
    link_vendor_code_table: str = "hdfs:///user/andreyyur/project/df_link_vendor_code.parquet"
    user_vendor_code_table: str = "hdfs:///user/andreyyur/project/df_user_vendor_code.parquet"


app_settings = AppSettings()
