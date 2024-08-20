# Import necessary libraries
import requests
import hashlib
import hmac
import os
from datetime import datetime
from dotenv import load_dotenv
import schedule
import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


# Database connection setup
host = 'localhost'
port = 5432 
user = 'postgres'
password = os.getenv('PASSWORD_DB')
db = 'postgres' 
conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
connection_string = f"postgresql://{user}:{password}@{host}/{db}"
engine = create_engine(connection_string)


# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN_fastex')
CHAT_ID = os.getenv('CHAT_ID_fastex')
PUBLIC_KEY = os.getenv('PUBLIC_KEY_fastex')
PRIVATE_KEY = os.getenv('PRIVATE_KEY_fastex')


# Function to send a message via Telegram
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


# Function to fetch balance from the exchange
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
    elif response.status_code == 405:
        return "Method Not Allowed"
    else:
        print(f'Error: {response.status_code} - {response.text}')
        return 'N/A'


# Function to get the current date and time in formatted strings
def get_formatted_datetime():
    now = datetime.now()
    formatted_date = now.strftime("%B %d, %Y")
    formatted_time = now.strftime("%H:%M:%S")
    return formatted_date, formatted_time

# Function to update the DataFrame and send the message
def send():
    global df
    total_balance = fetch_balance()
    formatted_date, formatted_time = get_formatted_datetime()
    
    token = 'SHIB'
    
    if pd.isna(df.iloc[-1]['Total Balance']):
        df.at[df.index[-1], 'Total Balance'] = 0

    df['Total Balance'] = pd.to_numeric(df['Total Balance'], errors='coerce')

    if total_balance == 'N/A' or df.iloc[-1]['Total Balance']=='N/A':
        diff = 0
    else:
        diff = float(total_balance) - df.iloc[-1]['Total Balance']  

    new_row = pd.DataFrame({
    'Date': [formatted_date],
    'Time': [formatted_time],
    'Token': [token],
    'Total Balance': [total_balance],
    'Diff': [diff]
    })

    df = pd.concat([df, new_row], ignore_index=True)
    
    latest_row = df.iloc[-1]
    message = (f"Date: {latest_row['Date']}\n"
               f"Time: {latest_row['Time']}\n"
               f"Token: {latest_row['Token']}\n"
               f"Total Balance: {latest_row['Total Balance']}\n"
               f"Diff: {latest_row['Diff']}")
    

    df.to_sql(name='df_balances',
              con=engine,
              index=False,
              if_exists='replace')

    print(message)
    send_message(message)

# Schedule the `send` function to run every hour
schedule.every().hour.at(":00").do(send)

# Keep the script running and check for scheduled tasks
while True:
    schedule.run_pending()
    time.sleep(1)
