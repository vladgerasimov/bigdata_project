import telebot
import json
from telebot import types
from PageInfo import get_page_info
import re
from pyspark.sql import SparkSession

from SparkUpdateTables import update_df_user_vendor_code, update_df_link_vendor_code
from SparkUpdateTables import check_df_user_vendor_code, check_df_link_vendor_code
from analytics.prices import get_vendor_code_by_link, get_price_history, save_prices_plot

with open("secrets/bot_secrets.json", 'r') as f:
    bot_secret = json.load(f)['token']

bot = telebot.TeleBot(bot_secret)
pattern = r'https?://\S+'

spark = SparkSession.builder\
                .master("local[*]")\
                .appName('yurkin_tg_bot')\
                .getOrCreate()


def menu(chat_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    markup.row(types.KeyboardButton('➕Добавить товар'))
    markup.row(types.KeyboardButton('📈Показать график цены товара'))
    bot.send_message(chat_id, text='Меню:', reply_markup=markup)


@bot.message_handler(commands=['start'])
def main(message):
    bot.send_message(message.chat.id, 'Добрый день! Мы поможем тебе купить товар по лучшей цене!')
    menu(message.chat.id)


@bot.message_handler(func=lambda msg: msg.text == '➕Добавить товар')
def get_link(message):
    chat_id = message.chat.id
    bot.send_message(chat_id, "Отправьте ссылку товара ЗЯ для добавления в список отслеживания")
    bot.register_next_step_handler_by_chat_id(chat_id, get_link_callback)


def get_link_callback(message):
    user_id = message.from_user.id
    if 'goldapple.ru' in message.text:
        link = re.findall(pattern,message.text)[0]
        info_link = get_page_info(link)
        vendor_code = int(info_link[0])
        goods_name = info_link[1]
        user_vendor_code_update = (user_id, vendor_code)
        link_vendor_code_update = (link, vendor_code, goods_name)
        bot.send_message(
            user_id,
            "Введите процент скидки, при котором нам оповестить вас. Например, 25. "
            "Если не хотите получать оповещения, введите -"
        )
        bot.register_next_step_handler_by_chat_id(message.chat.id, get_min_discount, user_vendor_code_update=user_vendor_code_update)
        if check_df_link_vendor_code(link_vendor_code_update, spark=spark) < 1:
            print("--------> check link_vendor ok")
            update_df_link_vendor_code(link_vendor_code_update, spark=spark)
            print("--------> update link_vendor ok")
    else:
        bot.send_message(user_id, "Это не похоже на ссылку ЗЯ (((")


def get_min_discount(message, user_vendor_code_update: tuple):
    user_id = message.from_user.id
    discount_percent = message.text.replace("%", "")
    if discount_percent.isdigit() or discount_percent == "-":
        row = (*user_vendor_code_update, int(discount_percent) if discount_percent else None)
        if check_df_user_vendor_code(row, spark=spark) < 1:
            print("--------> check user_vendor ok")
            bot.send_message(
                user_id,
                f"Записываем ваши пожелания в базу..."
            )
            update_df_user_vendor_code(row, spark=spark)
            print("--------> update user_vendor ok")
            bot.send_message(
                user_id,
                "Спасибо! Теперь я знаю, что вам интересно отслеживать. "
                "Чтобы узнать последнюю запись о товаре воспользуйтесь кнопкой /???"
            )
    else:
        bot.send_message(
            user_id,
            "Неверный формат ввода: введите целое число процентов, например: 20"
        )
        bot.register_next_step_handler(message, get_min_discount)


@bot.message_handler(func=lambda msg: msg.text == '📈Показать график цены товара')
def plot_prices(message):
    user_id = message.from_user.id
    bot.send_message(user_id, "Отправьте ссылку на товар, цена которого интересует")
    bot.register_next_step_handler_by_chat_id(user_id, plot_prices_callback)


def plot_prices_callback(message):
    user_id = message.from_user.id
    link = re.findall(pattern, message.text)[0]
    vendor_code = get_vendor_code_by_link(link, spark)
    if not vendor_code:
        bot.send_message(user_id, "У нас нет данных о цене этого товара☹️")
    else:
        price_history = get_price_history(vendor_code, spark)
        if not price_history:
            bot.send_message(user_id, "У нас нет данных о цене этого товара☹️")
        else:
            plot_path = save_prices_plot(price_history)
            with open(plot_path, "rb") as image:
                bot.send_photo(user_id, image)
            plot_path.unlink()

bot.infinity_polling()
