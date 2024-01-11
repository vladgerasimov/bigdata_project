import telebot
import json
from PageInfo import get_page_info
import re
from SparkUpdateTables import update_df_user_vendor_code, update_df_link_vendor_code
from SparkUpdateTables import check_df_user_vendor_code, check_df_link_vendor_code


with open("project/secrets/bot_secrets.json", 'r') as f:
    bot_secret = json.load(f)['token']

bot = telebot.TeleBot(bot_secret)
pattern = r'https?://\S+'

@bot.message_handler(commands=['start'])
def main(message):
    bot.send_message(message.chat.id, 'Добрый день! Отправьте ссылку товара ЗЯ для добавления в список отслеживания')


@bot.message_handler()
def get_link(message):
    if 'goldapple.ru' in message.text:
        link = re.findall(pattern,message.text)[0]
        info_link = get_page_info(link)
        vendor_code = info_link[0]
        goods_name = info_link[1]
        user_id = message.from_user.id
        user_vendor_code_update = tuple([user_id, vendor_code])
        link_vendor_code_update = tuple([link, vendor_code, goods_name])
        bot.send_message(message.chat.id,
                         f"Записываем ваши пожелания в базу...")

        if check_df_user_vendor_code(user_vendor_code_update) < 1:
            update_df_user_vendor_code(user_vendor_code_update)

        if check_df_link_vendor_code(link_vendor_code_update) < 1:
            update_df_link_vendor_code(link_vendor_code_update)

        bot.send_message(message.chat.id,
                         f"Спасибо! Теперь я знаю, что вам интересно отслеживать. Чтобы узнать последнюю запись о товаре воспользуйтесь кнопкой /???")
    else:
        bot.send_message(message.chat.id, "Это не похоже на ссылку ЗЯ (((")


bot.infinity_polling()
