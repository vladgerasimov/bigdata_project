import telebot
import json
from PageInfo import get_page_info
import re
from pyspark.sql import SparkSession

from SparkUpdateTables import update_df_user_vendor_code, update_df_link_vendor_code
from SparkUpdateTables import check_df_user_vendor_code, check_df_link_vendor_code


with open("secrets/bot_secrets.json", 'r') as f:
    bot_secret = json.load(f)['token']

bot = telebot.TeleBot(bot_secret)
pattern = r'https?://\S+'

spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_tg_bot')\
                .getOrCreate()

@bot.message_handler(commands=['start'])
def main(message):
    bot.send_message(message.chat.id, 'Добрый день! Отправьте ссылку товара ЗЯ для добавления в список отслеживания')


@bot.message_handler()
def get_link(message):
    if 'goldapple.ru' in message.text:
        link = re.findall(pattern,message.text)[0]
        info_link = get_page_info(link)
        vendor_code = int(info_link[0])
        goods_name = info_link[1]
        user_id = message.from_user.id
        user_vendor_code_update = (user_id, vendor_code)
        link_vendor_code_update = (link, vendor_code, goods_name)
        print(f"{user_vendor_code_update=}")
        print(f"{link_vendor_code_update=}")
        bot.send_message(
            message.chat.id,
            "Введите процент скидки, при котором нам оповестить вас. Например, 25. "
            "Если не хотите получать оповещения, введите -"
        )

        if check_df_link_vendor_code(link_vendor_code_update, spark=spark) < 1:
            print("--------> check link_vendor ok")
            update_df_link_vendor_code(link_vendor_code_update, spark=spark)
            print("--------> update link_vendor ok")
        bot.register_next_step_handler_by_chat_id(message.chat.id, get_min_discount, user_vendor_code_update=user_vendor_code_update)
    else:
        bot.send_message(message.chat.id, "Это не похоже на ссылку ЗЯ (((")


def get_min_discount(message, user_vendor_code_update: tuple):
    discount_percent = message.text.replace("%", "")
    if discount_percent.isdigit() or discount_percent == "-":
        row = (*user_vendor_code_update, int(discount_percent) if discount_percent else None)
        if check_df_user_vendor_code(row, spark=spark) < 1:
            print("--------> check user_vendor ok")
            bot.send_message(
                message.chat.id,
                f"Записываем ваши пожелания в базу..."
            )
            update_df_user_vendor_code(row, spark=spark)
            print("--------> update user_vendor ok")
            bot.send_message(
                message.chat.id,
                "Спасибо! Теперь я знаю, что вам интересно отслеживать. "
                "Чтобы узнать последнюю запись о товаре воспользуйтесь кнопкой /???"
            )
    else:
        bot.send_message(
            message.chat.id,
            "Неверный формат ввода: введите целое число процентов, например: 20"
        )
        bot.register_next_step_handler(message, get_min_discount)




bot.infinity_polling()
