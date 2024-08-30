import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime, timedelta
import time
from telegram import Bot
from telegram.error import TelegramError
import asyncio
import logging
from httpx import AsyncClient
import traceback

telegram_bot_token = "7384843477:AAFsitozSLRZvyFAuu_ZSEVSm1st_cnC0DA"
telegram_channel_id = "@SportPrognoze2"
telegram_id = "7449420157"
logging.basicConfig(level=logging.ERROR)
client = AsyncClient(timeout=30)
bot = Bot(token=telegram_bot_token)
last_pinned_message_id = None


async def send_telegram_message(message, pin=False, chat_id=telegram_channel_id):
    global last_pinned_message_id
    try:
        # Ограничение длины сообщения
        max_length = 4096
        if len(message) > max_length:
            # Делим сообщение по пробелам, чтобы избежать разрывов слов
            words = message.split(' ')
            parts = []
            part = ""
            for word in words:
                if len(part) + len(word) + 1 <= max_length:
                    part += word + " "
                else:
                    parts.append(part.strip())
                    part = word + " "
            if part:
                parts.append(part.strip())

            for part in parts:
                await bot.send_message(chat_id=chat_id, text=part)
        else:
            sent_message = await bot.send_message(chat_id=chat_id, text=message)

            if pin and chat_id == telegram_channel_id:
                if last_pinned_message_id:
                    try:
                        await bot.unpin_chat_message(chat_id=telegram_channel_id, message_id=last_pinned_message_id)
                    except TelegramError as e:
                        logging.error(f"Failed to unpin old message: {e}")
                await bot.pin_chat_message(chat_id=telegram_channel_id, message_id=sent_message.message_id,
                                           disable_notification=True)
                last_pinned_message_id = sent_message.message_id
    except Exception as e:
        logging.error(f"Failed to send message: {e}")
        await bot.send_message(chat_id=telegram_id, text=f"Ошибка при отправке сообщения: {str(e)}")
        await asyncio.sleep(30)


# Функция для парсинга страницы беттеров и получения ссылок
def parse_betters(url):
    try:
        response = requests.get(url)
        html_content = response.content
        soup = BeautifulSoup(html_content, 'html.parser')
        forecasters = soup.find_all('a', class_='forecaster-rating__item')
        links = []
        for forecaster in forecasters:
            try:
                forecaster_url = forecaster['href']
                profit_str = forecaster.find('span', class_='forecaster-rating__item-badge').text.strip().replace('%', '')
                profit = float(profit_str)
                if profit >= 30:
                    links.append(forecaster_url)
            except Exception as e:
                error_message = f"Error in parse_betters function, processing forecaster: {forecaster}\nError: {str(e)}"
                logging.error(error_message)
                # Отправка сообщения в бот с деталями ошибки
                asyncio.run(send_telegram_message(error_message, chat_id=telegram_id))
        return links
    except Exception as e:
        error_message = f"Error in parse_betters function when requesting URL: {url}\nError: {str(e)}"
        logging.error(error_message)
        # Отправка сообщения в бот с деталями ошибки
        asyncio.run(send_telegram_message(error_message, chat_id=telegram_id))
        return []


# Функция для парсинга ссылок на ставки для каждого беттера
def parse_bets(better_url):
    try:
        tip_response = requests.get(better_url)
        tip_html_content = tip_response.text
        tip_soup = BeautifulSoup(tip_html_content, 'html.parser')
        lasttips = tip_soup.find('div', id='lasttips')
        bets_links = []
        if lasttips:
            for bet in lasttips.find_all('div', class_='mini-tip'):
                try:
                    if 'is-draw' in bet['class']:
                        teams_link = bet.find('a', class_='mini-tip__teams')
                        if teams_link:
                            link = teams_link['href']
                            bets_links.append(link)
                except Exception as e:
                    error_message = f"Error in parse_bets function, processing bet: {bet}\nError: {str(e)}"
                    logging.error(error_message)
                    # Отправка сообщения в бот с деталями ошибки
                    asyncio.run(send_telegram_message(error_message, chat_id=telegram_id))
        return bets_links
    except Exception as e:
        error_message = f"Error in parse_bets function when requesting URL: {better_url}\nError: {str(e)}"
        logging.error(error_message)
        # Отправка сообщения в бот с деталями ошибки
        asyncio.run(send_telegram_message(error_message, chat_id=telegram_id))
        return []


# Функция для парсинга данных ставок
def parse_bet_data(bet_url):
    try:
        response = requests.get(bet_url)
        response.raise_for_status()  # Добавлено для обработки HTTP ошибок
        soup = BeautifulSoup(response.text, 'html.parser')

        # Проверяем наличие дива с результатом
        result_element = soup.select_one('div.vp-forecast-bet__value-bank.vp-forecast-bet__value-bank-result')
        print(result_element)
        outcome_type = None
        if result_element:
            try:
                if 'is-default' in result_element.get('class', []):
                    outcome_type = "draw"
                elif 'is-up' in result_element.get('class', []):
                    outcome_type = "win"
                elif 'is-down' in result_element.get('class', []):
                    outcome_type = "lose"

                return {
                    'outcome_type': outcome_type,
                    'status': 'done'
                }
            except Exception as e:
                error_message = f"Error in parse_bet_data function, processing result_element: {result_element}\nError: {str(e)}"
                logging.error(error_message)
                asyncio.run(send_telegram_message(error_message, chat_id=telegram_id))
                return None

        else:
            # Если элемента нет, собираем данные для нового прогноза
            try:
                event_datetime = soup.find('time', class_='vp-match-card-content__info-match-date')['datetime']
                outcome_type = "new"
                link = bet_url
                sides = soup.find('h1', class_='site-title site-title_h2').text
                sport = soup.find('b', class_='vp-match-card-content__info-match-type').text
                league = soup.find('span', class_='vp-match-card-content__info-match-league').text
                bet_title = soup.find('div', class_='vp-forecast-bet__title')
                stake = bet_title.find('a').text if bet_title else ''
                stake_element = bet_title.find_all('a')[1] if bet_title and len(bet_title.find_all('a')) > 1 else None
                odds = float(stake_element.text) if stake_element else 0.0
                description_id = f'news-id-{bet_url.split("/")[-1].split("-")[0]}'
                description_element = soup.find('div', id=description_id)
                description = description_element.text.strip() if description_element else ''
                return {
                    'event_datetime': event_datetime,
                    'outcome_type': outcome_type,
                    'link': link,
                    'sport': sport,
                    'sides': sides,
                    'league': league,
                    'stake': stake,
                    'odds': odds,
                    'status': 'wait',
                    'description': description
                }
            except Exception as e:
                error_message = f"Error in parse_bet_data function, processing new bet data: {bet_url}\nError: {str(e)}"
                logging.error(error_message)
                asyncio.run(send_telegram_message(error_message, chat_id=telegram_id))
                return None
    except Exception as e:
        error_message = f"Error in parse_bet_data function when requesting URL: {bet_url}\nError: {str(e)}"
        logging.error(error_message)
        asyncio.run(send_telegram_message(error_message, chat_id=telegram_id))
        return None


# Функция для создания таблицы в БД
def create_table():
    conn = sqlite3.connect('bets.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS bets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        created DATETIME, 
                        updated DATETIME, 
                        event_datetime DATETIME, 
                        outcome_type TEXT, 
                        link TEXT,
                        sport TEXT,
                        sides TEXT,
                        league TEXT, 
                        stake TEXT, 
                        odds REAL, 
                        status TEXT, 
                        description TEXT
                    )''')
    conn.commit()
    conn.close()


def record_exists(conn, link, sides, stake):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT 1 FROM bets WHERE link = ? OR (sides = ? AND stake = ?)
    ''', (link, sides, stake))
    return cursor.fetchone() is not None


# Функция для записи данных в БД
async def insert_bet_data(data):
    conn = sqlite3.connect('bets.db')
    try:
        if not record_exists(conn, data['link'], data['sides'], data['stake']) and data['odds'] < 2.5:
            cursor = conn.cursor()
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data['created'] = current_time
            data['updated'] = current_time
            cursor.execute('''
                    INSERT INTO bets (created, updated, event_datetime, outcome_type, link, sport, sides, league, stake, odds, status, description)
                    VALUES (:created, :updated, :event_datetime, :outcome_type, :link, :sport, :sides, :league, :stake, :odds, :status, :description)
                ''', data)
            conn.commit()
            print(f"Saved bet: {data['link']}")
            # Форматирование даты и времени для сообщения
            event_datetime_str = data['event_datetime']
            event_datetime = datetime.strptime(event_datetime_str, "%Y-%m-%d %H:%M")
            formatted_date_time = event_datetime.strftime("%d.%m.%Y %H:%M")
            message = (
                f"{formatted_date_time} МСК\n"
                f"{data['sport']}\n"
                f"{data['league']}\n"
                f"{data['sides']}\n"
                f"{data['stake']} - {data['odds']}\n"
                f"{data['description']}"
            )
            await send_telegram_message(message)
        else:
            print(f"Bet already exists: {data['link']}")
    finally:
        conn.close()


# Функция для обновления статуса в БД
def update_bet_status(bet_id, outcome_type, status):
    conn = sqlite3.connect('bets.db')
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE bets
        SET outcome_type = ?, status = ?, updated = ?
        WHERE id = ?
    ''', (outcome_type, status, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), bet_id))
    conn.commit()
    conn.close()


def fetch_pending_bets():
    conn = sqlite3.connect('bets.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id, link FROM bets WHERE status = 'wait'")
    bets = cursor.fetchall()
    conn.close()
    return bets


def fetch_bet_by_link(link):
    conn = sqlite3.connect('bets.db')
    cursor = conn.cursor()
    cursor.execute("SELECT id, outcome_type, sport, league, sides, stake, odds FROM bets WHERE link = ?", (link,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return {'id': result[0],
                'outcome_type': result[1],
                'sport': result[2],
                'league': result[3],
                'sides': result[4],
                'stake': result[5],
                'odds': result[6],
                }
    return None


#⚽️🏀🏈⚾️🥎🎾🏐🏉🥏🎱🪀🏓🏸🏒🏑🥍🏏⛳️🥊⛸⛷
async def process_pending_bets():
    bets = fetch_pending_bets()
    update_occurred = False
    for bet_id, link in bets:
        bet_data = parse_bet_data(link)
        print(bet_data)
        if isinstance(bet_data, dict) and 'outcome_type' in bet_data:  # Только если присутствует исход
            current_bet = fetch_bet_by_link(link)  # Получаем текущие данные ставки из базы по ссылке
            print(current_bet)
            if current_bet and current_bet['outcome_type'] != bet_data['outcome_type']:  # Проверяем изменился ли исход
                update_bet_status(bet_id, bet_data['outcome_type'], bet_data['status'])
                outcome_emoji = "✅" if bet_data['outcome_type'] == 'win' else "❌"
                message = (
                    f"{current_bet['sport']}\n"
                    f"{current_bet['league']}\n"
                    f"{current_bet['sides']}\n"
                    f"{current_bet['stake']} - {current_bet['odds']}\n"
                    f"{outcome_emoji}"
                )
                await send_telegram_message(message)
                update_occurred = True
            else:
                print(f"No change in outcome for bet {link}")
        else:
            print("Error processing bet data:", bet_data)
    if update_occurred:
        daily_stats = get_statistics(0)
        weekly_stats = get_statistics(6)
        monthly_stats = get_statistics(29)
        stats_message = (
            f"Сегодня: {daily_stats[0]} ставок, {daily_stats[1]} выиграло, {daily_stats[2]}% побед\n"
            f"За неделю: {weekly_stats[0]} ставок, {weekly_stats[1]} выиграло, {weekly_stats[2]}% побед\n"
            f"За месяц: {monthly_stats[0]} ставок, {monthly_stats[1]} выиграло, {monthly_stats[2]}% побед")
        await send_telegram_message(stats_message, pin=True)


def get_statistics(days):
    conn = sqlite3.connect('bets.db')
    cursor = conn.cursor()
    end_date = datetime.now()
    start_date = (end_date - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute('''
        SELECT COUNT(*), 
               SUM(CASE WHEN outcome_type = 'win' THEN 1 ELSE 0 END)
        FROM bets
        WHERE created BETWEEN ? AND ?
          AND outcome_type IN ('win', 'lose', 'draw')
    ''', (start_date_str, end_date_str))
    total_bets, total_wins = cursor.fetchone()
    print(start_date_str)
    print(end_date_str)
    if total_bets == 0:
        total_wins = 0
    win_percentage = (total_wins / total_bets * 100) if total_bets else 0
    return total_bets, total_wins, round(win_percentage)


# Основная функция
async def main():
    try:
        # Создаем таблицу, если нет
        create_table()
        # Парсим ссылки беттеров
        betters_links = parse_betters('https://vprognoze.kz/statalluser/')
        print(betters_links)
        for better_link in betters_links:
            # Парсим ссылки на ставки
            bets_links = parse_bets(better_link)
            print(bets_links)
            for bet_link in bets_links:
                # Парсим данные ставки и сохраняем в БД
                bet_data = parse_bet_data(bet_link)
                print(bet_data)
                if isinstance(bet_data, dict) and 'link' in bet_data and 'sides' in bet_data and 'stake' in bet_data:
                    await insert_bet_data(bet_data)
        await process_pending_bets()
    finally:
        await client.aclose()  # Закрытие клиента


if __name__ == "__main__":
    asyncio.run(main())
