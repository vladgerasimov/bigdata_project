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

    return tuple([code, goods_name.replace("'", "`"), price, current_datetime.strftime("%Y-%m-%d %H:%M:%S")])

# link = 'https://goldapple.ru/19000208704-visnevyj-cvet'
# link = 'https://goldapple.ru/19000186448-airpods-pro-2'

# print(get_page_info(url=link))
