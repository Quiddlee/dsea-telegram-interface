import logging
from io import BytesIO

import requests
import telebot
from decouple import config
from telebot import types
from telebot.types import InputMediaDocument

from bot.db import get_call_schedule, save_call_schedule
from bot.efficiency import percent_t_avg
from bot.keep_alive import keep_alive
from parsing.main import (call_schedule_parser, class_schedule_parser, rating_list_parser, scholarship_list_parser,
                          session_schedule_parser, timetable_calendar_parser)

TOKEN = config('TOKEN')
bot = telebot.TeleBot(TOKEN)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

schedule_cache = {}
session_cache = {}

COURSE_LABELS = [
    "1 курс", "2 курс", "3 курс", "4 курс",
    "Прискорений курс (2 семестр)",
    "Прискорений курс (4 семестр)",
    "Магістри (1 курс)"
]

SESSION_COURSE_LABELS = [
    "1 куpс", "2 куpс", "3 куpс", "4 куpс",
    "Прискоpений курс (2 семестр)", "Прискоpений курс (4 семестр)"
]

ABOUT = f"""
📚 Цей Telegram-застосунок створено для студентів і працівників Донбаської державної машинобудівної академії. Його мета — забезпечити швидкий, зручний і ефективний доступ до навчальної інформації.

🔍 Замість самостійного пошуку на сайтах академії чи кафедр, користувачі можуть отримати частину найважливішої інформації — розклади, документи та посилання — безпосередньо в Telegram. Бот автоматично збирає й обробляє доступні дані, що значно спрощує процес і заощаджує час.

📈 У деяких випадках ефективність використання бота — на {percent_t_avg:.1f}% вища за користування сайтом у плані витраченого часу.

💻 Проєкт реалізовано студентом кафедри «Інтелектуальних систем прийняття рішень», спеціальності «Інформаційні системи та технології». Науковий керівник проєкту — кандидат технічних наук, доцент, в. о. зав. вищезгаданої кафедри Олександр Юрійович Мельников.
"""


@bot.message_handler(commands=['start'])
def send_bot_menu(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)

    button1 = types.KeyboardButton('Moodle')
    button2 = types.KeyboardButton('Web-ресурси та соціальні мережі ДДМА')
    button3 = types.KeyboardButton('Розклад дзвінків')
    button4 = types.KeyboardButton('Розклад занять')
    button5 = types.KeyboardButton('Розклад сесії')
    button6 = types.KeyboardButton('Рейтинг студентів')
    button7 = types.KeyboardButton('Стипендіальний список')
    button8 = types.KeyboardButton('Табель-календар')
    button9 = types.KeyboardButton('About')

    markup.add(button1, button2, button3, button4, button5, button6, button7, button8, button9)

    bot.send_message(
        message.chat.id,
        '🤖 Вітаю! Я Telegram-бот ДДМА, створений для зручного та швидкого доступу до навчальної інформації. Допоможу знайти розклад, важливі документи та корисні посилання.',
        reply_markup=markup
    )

# -----------------------------------------------------------------------------------
# ----------------------------- THE DIFFERENT APPROACH ------------------------------


# @bot.message_handler(commands=['call_schedule'])
# def send_call_schedule(message):
#     text, image_url, page_url = call_schedule_parser()
#     text = "\n".join(text) + "\n\n" + f'Джерело: {page_url}'
#     response = requests.get(image_url, stream=True)

#     bot.send_photo(message.chat.id, response.raw, caption=text)


# @bot.message_handler(commands=['call_schedule_db'])
# def send_call_schedule_db(message):
#     try:
#         content = get_call_schedule()
#         bot.send_message(message.chat.id, content)
#     except Exception as e:
#         logger.error("ERROR!!!", exc_info=e)
#         bot.send_message(message.chat.id, "⚠️ На жаль, не вдалося отримати актуальні дані з бази даних!")


# @bot.message_handler(commands=['update_call_schedule_db'])
# def update_call_schedule_db(message):
#     bot.send_message(message.chat.id, "⏳ Оновлюю інформацію...")

#     try:
#         text, image_url, page_url = call_schedule_parser()
#         save_call_schedule(text)
#         bot.send_message(message.chat.id, "✅ Оновлений розклад дзвінків успішно збережено у базі даних!")
#     except Exception as e:
#         logger.error("ERROR!!!", exc_info=e)
#         bot.send_message(message.chat.id, f"⚠️ На жаль, оновити дані не вдалося: {e}")

# ----------------------------- THE DIFFERENT APPROACH ------------------------------
# -----------------------------------------------------------------------------------


@bot.message_handler(commands=['call_schedule'])
def send_call_schedule(message):
    chat_id = message.chat.id

    try:
        text_lines, image_url, page_url = call_schedule_parser()
        save_call_schedule(text_lines)

        caption = "\n".join(text_lines) + f"\n\nДжерело: {page_url}"

        resp = requests.get(image_url, stream=True, timeout=5)
        resp.raise_for_status()

        bot.send_photo(chat_id, resp.raw, caption=caption)
    except Exception as e:
        logger.error("ERROR!!!", exc_info=e)
        bot.send_message(
            chat_id, f"⚠️ На жаль, не вдалося отримати актуальні дані з вебсайту! Отримую інформацію з бази даних...")

        try:
            content = get_call_schedule()
            bot.send_message(chat_id, content)
        except Exception as db_e:
            logger.error("ERROR!!!", exc_info=db_e)
            bot.send_message(chat_id, "⚠️ На жаль, не вдалося отримати актуальні дані з бази даних!")

# -----------------------------------------------------------------------------------


@bot.message_handler(commands=['class_schedule'])
def send_class_schedule(message):
    text, image_urls, page_url = class_schedule_parser()
    text = f"{text}\n\nДжерело: {page_url}"

    for image_url in image_urls:
        response = requests.get(image_url)

        if response.status_code == 200:
            image_data = BytesIO(response.content)
            bot.send_document(message.chat.id, document=image_data, caption=text)
        else:
            bot.send_message(message.chat.id, f"⚠️ На жаль, виникла помилка: {image_url}")

# -----------------------------------------------------------------------------------


@bot.message_handler(content_types=['text'])
def bot_message(message):
    if message.chat.type != 'private':
        pass

    chat_id = message.chat.id
    file_text = message.text

# -----------------------------------------------------------------------------------

    def go_to_website(msg, link):
        inline = types.InlineKeyboardMarkup()
        btn = types.InlineKeyboardButton(text=msg.text, url=link)
        inline.add(btn)

        bot.send_message(msg.chat.id, "🔗 Посилання на ресурс:", reply_markup=inline)

# -----------------------------------------------------------------------------------

    if file_text == 'Назад':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)

        button1 = types.KeyboardButton('Moodle')
        button2 = types.KeyboardButton('Web-ресурси та соціальні мережі ДДМА')
        button3 = types.KeyboardButton('Розклад дзвінків')
        button4 = types.KeyboardButton('Розклад занять')
        button5 = types.KeyboardButton('Розклад сесії')
        button6 = types.KeyboardButton('Рейтинг студентів')
        button7 = types.KeyboardButton('Стипендіальний список')
        button8 = types.KeyboardButton('Табель-календар')
        button9 = types.KeyboardButton('About')

        markup.add(button1, button2, button3, button4, button5, button6, button7, button8, button9)

        bot.send_message(
            message.chat.id, '🔙 Назад', reply_markup=markup)

# -----------------------------------------------------------------------------------

    if file_text == 'Web-ресурси та соціальні мережі ДДМА':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        button1 = types.KeyboardButton('Офіційний Сайт')
        button2 = types.KeyboardButton('YouTube')
        button3 = types.KeyboardButton('Telegram')
        button4 = types.KeyboardButton('Telegram-чат')
        button5 = types.KeyboardButton('LinkedIn')
        button6 = types.KeyboardButton('Instagram')
        button7 = types.KeyboardButton('Facebook')
        button8 = types.KeyboardButton('Facebook: Медіа-Група ДДМА')
        button9 = types.KeyboardButton('Кафедра ІСПР')
        button10 = types.KeyboardButton('Назад')

        markup.add(button1, button2, button3, button4, button5, button6, button7, button8, button9, button10)
        bot.send_message(chat_id, '✅ Виберіть одну з опцій:', reply_markup=markup)

    if file_text == "Moodle":
        go_to_website(message, "http://moodle-new.dgma.donetsk.ua/")

    if file_text == "Офіційний Сайт":
        go_to_website(message, "http://www.dgma.donetsk.ua/")

    if file_text == "YouTube":
        go_to_website(message, "https://www.youtube.com/user/mediagrupaAcademia")

    if file_text == "Telegram":
        go_to_website(message, "https://t.me/ddma_official")

    if file_text == "Telegram-чат":
        go_to_website(message, "https://bit.ly/36Wc2kB")

    if file_text == "LinkedIn":
        go_to_website(message, "https://www.linkedin.com/school/donbas-state-engineering-academy-dsea/")

    if file_text == "Instagram":
        go_to_website(message, "https://www.instagram.com/ddma_official/")

    if file_text == "Facebook":
        go_to_website(message, "https://www.facebook.com/ddma.kramatorsk/")

    if file_text == "Facebook: Медіа-Група ДДМА":
        go_to_website(message, "https://www.facebook.com/groups/mediagrupa/")

    if file_text == "Кафедра ІСПР":
        go_to_website(message, "http://www.dgma.donetsk.ua/~kiber/")

# -----------------------------------------------------------------------------------

    if file_text == 'About':
        bot.send_message(message.chat.id, ABOUT)

# -----------------------------------------------------------------------------------

    if file_text == 'Розклад дзвінків':
        bot.send_message(message.chat.id, '⏳ Отримую інформацію...')
        send_call_schedule(message)

# -----------------------------------------------------------------------------------

    if file_text == 'Розклад занять':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        btn1 = types.KeyboardButton('1 курс')
        btn2 = types.KeyboardButton('2 курс')
        btn3 = types.KeyboardButton('3 курс')
        btn4 = types.KeyboardButton('4 курс')
        btn5 = types.KeyboardButton('Прискорений курс (2 семестр)')
        btn6 = types.KeyboardButton('Прискорений курс (4 семестр)')
        btn7 = types.KeyboardButton('Магістри (1 курс)')
        btn8 = types.KeyboardButton('Назад')
        markup.add(btn1, btn2, btn3, btn4, btn5, btn6, btn7, btn8)

        bot.send_message(chat_id, '✅ Виберіть одну з опцій:', reply_markup=markup)

        title, image_urls, page_url = class_schedule_parser()
        schedule_cache[chat_id] = (title, image_urls, page_url)

    if file_text in COURSE_LABELS:
        if chat_id not in schedule_cache:
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
            markup.add(types.KeyboardButton('Розклад занять'))

            bot.send_message(chat_id, '🔄 Будь ласка, натисніть "Розклад занять". Йде обробка даних...',
                             reply_markup=markup)
            return

        bot.send_message(chat_id, '⏳ Отримую інформацію...')

        title, image_urls, page_url = schedule_cache[chat_id]
        idx = COURSE_LABELS.index(file_text)

        try:
            url = image_urls[idx]
        except (IndexError, TypeError):
            bot.send_message(chat_id, "⚠️ На жаль, виникла помилка!")

        resp = requests.get(url)

        if resp.status_code == 200:
            bio = BytesIO(resp.content)
            filename = f"Розклад занять - {file_text}.png"
            bio.name = filename
            caption = f"{file_text} | {title} \n\nДжерело: {page_url}"
            bot.send_document(chat_id, document=bio, caption=caption)
        else:
            bot.send_message(chat_id, f"⚠️ На жаль, виникла помилка: {url}")

# -----------------------------------------------------------------------------------

    if file_text == 'Розклад сесії':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        btn1 = types.KeyboardButton('1 куpс')
        btn2 = types.KeyboardButton('2 куpс')
        btn3 = types.KeyboardButton('3 куpс')
        btn4 = types.KeyboardButton('4 куpс')
        btn5 = types.KeyboardButton('Прискоpений курс (2 семестр)')
        btn6 = types.KeyboardButton('Прискоpений курс (4 семестр)')
        btn7 = types.KeyboardButton('Назад')
        markup.add(btn1, btn2, btn3, btn4, btn5, btn6, btn7)

        bot.send_message(chat_id, '✅ Виберіть одну з опцій:', reply_markup=markup)

        title, image_urls, page_url = session_schedule_parser()
        session_cache[chat_id] = (title, image_urls, page_url)

    if file_text in SESSION_COURSE_LABELS:
        if chat_id not in session_cache:
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
            markup.add(types.KeyboardButton('Розклад сесії'))
            bot.send_message(chat_id, '🔄 Будь ласка, натисніть "Розклад сесії". Йде обробка даних...',
                             reply_markup=markup)
            return

        bot.send_message(chat_id, '⏳ Отримую інформацію...')

        title, image_urls, page_url = session_cache[chat_id]
        idx = SESSION_COURSE_LABELS.index(file_text)

        try:
            url = image_urls[idx]
        except (IndexError, TypeError):
            bot.send_message(chat_id, "⚠️ На жаль, виникла помилка!")
            return

        resp = requests.get(url)

        if resp.status_code == 200:
            bio = BytesIO(resp.content)
            filename = f"Розклад сесії - {file_text.replace('Сесія — ', '')}.png"
            bio.name = filename
            caption = f"{file_text} | {title} \n\nДжерело: {page_url}"
            bot.send_document(chat_id, document=bio, caption=caption)
        else:
            bot.send_message(chat_id, f"⚠️ На жаль, виникла помилка: {url}")

# -----------------------------------------------------------------------------------

    if file_text == 'Рейтинг студентів':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        btn1 = types.KeyboardButton('ФАМІТ')
        btn2 = types.KeyboardButton('ФМ')
        btn3 = types.KeyboardButton('ФІТО')
        btn4 = types.KeyboardButton('ФЕМ')
        btn5 = types.KeyboardButton('Назад')
        markup.add(btn1, btn2, btn3, btn4, btn5)

        bot.send_message(chat_id, '✅ Виберіть одну з опцій:', reply_markup=markup)

    if file_text in ['ФАМІТ', 'ФМ', 'ФІТО', 'ФЕМ']:

        bot.send_message(chat_id, "⏳ Отримую інформацію...")

        try:
            rating_files, page_url = rating_list_parser()
            faculty_map = {
                'ФАМІТ': 'ФАМІТ',
                'ФМ': 'ФМ',
                'ФІТО': 'ФІТО',
                'ФЕМ': 'ФЕМ'
            }

            found = False

            for name, url in rating_files:
                if faculty_map[file_text] in name:
                    response = requests.get(url)

                    if response.status_code == 200:
                        file_data = BytesIO(response.content)
                        file_data.name = name
                        caption = f"{file_text} | Рейтинг студентів факультету\n\nДжерело: {page_url}"
                        bot.send_document(chat_id, file_data, caption=caption)
                        found = True
                        break

            if not found:
                bot.send_message(chat_id, f"⚠️ На жаль, виникла помилка: {file_text}.")

        except Exception as e:
            bot.send_message(chat_id, f"⚠️ На жаль, виникла помилка: {str(e)}")

# -----------------------------------------------------------------------------------

    if file_text == "Стипендіальний список":
        bot.send_message(chat_id, "⏳ Отримую інформацію...")

        try:
            file_url, file_name, file_text, page_url = scholarship_list_parser()
            response = requests.get(file_url)

            if response.status_code == 200:
                file_data = BytesIO(response.content)
                file_data.name = file_name
                caption = f"{file_text} \n\nДжерело: {page_url}"
                bot.send_document(chat_id, file_data, caption=caption)
            else:
                bot.send_message(chat_id, f"⚠️ На жаль, виникла помилка: {file_url}")

        except Exception as e:
            bot.send_message(chat_id, f"⚠️ На жаль, виникла помилка: {str(e)}")

# -----------------------------------------------------------------------------------

    if file_text == "Табель-календар":
        bot.send_message(chat_id, "⏳ Отримую інформацію...")

        try:
            title, files, page_url = timetable_calendar_parser()

            media_group = []

            for idx, (name, url) in enumerate(files, start=1):
                response = requests.get(url)

                if response.status_code == 200:
                    file_data = BytesIO(response.content)
                    file_data.name = name if name.endswith('.pdf') else f"{name}.pdf"

                    caption = f"{idx}) {name}" if idx == 1 else f"{idx}) {name}\n\nДжерело: {page_url}"
                    media_group.append(InputMediaDocument(media=file_data, caption=caption))
                else:
                    bot.send_message(chat_id, f"⚠️ На жаль, виникла помилка: {url}")
                    return

            bot.send_media_group(chat_id, media_group)

        except Exception as e:
            bot.send_message(chat_id, f"⚠️ На жаль, виникла помилка: {str(e)}")


bot.polling()
