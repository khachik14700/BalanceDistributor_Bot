import requests
import hashlib
import hmac
import os
from datetime import datetime
from dotenv import load_dotenv
import schedule
import time
load_dotenv()


BOT_TOKEN = os.getenv('BOT_TOKEN_fastex')
CHAT_ID = os.getenv('CHAT_ID_fastex')
PUBLIC_KEY = os.getenv('PUBLIC_KEY_fastex')
PRIVATE_KEY = os.getenv('PRIVATE_KEY_fastex')

def send_message(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print("Message sent successfully")
    else:
        print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")

def fetch_balance():
    url = 'https://exchange.fastex.com/api/v1/balance/info?asset=SHIB'
    post_data = "asset=SHIB"

    sign = hmac.new(
        PRIVATE_KEY.encode('utf-8'),
        post_data.encode('utf-8'),
        hashlib.sha512
    ).hexdigest()

    headers = {
        'Key': PUBLIC_KEY,
        'Sign': sign,
    }

    response = requests.post(url, headers=headers, data=post_data)

    if response.status_code == 200:
        data = response.json()
        total_balance = data.get('response', {}).get('entity', {}).get('total_balance', 'N/A')
        return total_balance
    else:
        print(f'Error: {response.status_code} - {response.text}')
        return 'N/A'


def get_formatted_datetime():
    now = datetime.now()
    formatted_date = now.strftime("%B %d, %Y")
    formatted_time = now.strftime("%H:%M:%S")
    return formatted_date, formatted_time

def send():
    total_balance = fetch_balance()
    formatted_date, formatted_time = get_formatted_datetime()
    message = (f"Date: {formatted_date}\n"
               f"Time: {formatted_time}\n"
               f"SHIB Total Balance: {total_balance}")
    print(message)
    send_message(message)

schedule.every().hour.at(":00").do(send)

while True:
    schedule.run_pending()
    time.sleep(1)
