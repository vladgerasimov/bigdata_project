import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def get_page_info(url:str):
    # url = 'https://goldapple.ru/65970100003-hydrating-essence'
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")

    raw_price = soup.find('div', class_='fMDws sRt8j')
    price = int(raw_price.contents[0].strip().replace(' ', ''))

    raw_goods_name = soup.find('div', class_='F42e9')
    goods_name = raw_goods_name.contents[0].strip()

    raw_code = soup.find('div', class_='ul6Oh')
    code = int(raw_code.contents[0].strip())

    current_datetime = datetime.now()+timedelta(hours=3)

    return tuple([code, goods_name.replace("'", "`"), price, current_datetime.strftime("%Y-%m-%d %H:%M:%S")])

# link = 'https://goldapple.ru/19000208704-visnevyj-cvet'

# print(get_page_info(url=link))
