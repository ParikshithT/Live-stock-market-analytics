import json
import requests
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import os

# Load configuration from config.json
with open('/app/config.json') as config_file:
    config = json.load(config_file)

# Access the API keys and database credentials
alpha_vantage_api_key = config['ALPHA_VANTAGE_API_KEY']
news_api_key = config['NEWS_API_KEY']
db_user = config['POSTGRES_USER']
db_password = config['POSTGRES_PASSWORD']
db_name = config['POSTGRES_DB']
db_host = os.getenv('POSTGRES_HOST', 'postgres')  # Use environment variable for hostname, default to 'postgres'

def main():
    # Function to get database engine
    def get_engine():
        return create_engine(f'postgresql://user:password@postgres:5432/stock_market_db')

    # Retry mechanism for database connection
    max_retries = 5
    retry_delay = 5  # in seconds

    for attempt in range(max_retries):
        try:
            engine = get_engine()
            with engine.connect() as connection:
                print("Database connection successful")
                break
        except OperationalError:
            if attempt < max_retries - 1:
                print(f"Database connection failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Database connection failed, max retries reached. Exiting.")
                raise


    # Create tables if they do not exist using SQLAlchemy 2.0 syntax
    with engine.connect() as connection:
        connection.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC
        );
        """)

        connection.execute("""
        CREATE TABLE IF NOT EXISTS news_data (
            id SERIAL PRIMARY KEY,
            title TEXT,
            description TEXT,
            content TEXT,
            published_at TIMESTAMP
        );
        """)

    def fetch_stock_data():
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AAPL&interval=1min&apikey={alpha_vantage_api_key}'
        response = requests.get(url)
        data = response.json()
        return data['Time Series (1min)']

    def fetch_news_data():
        url = f'https://newsapi.org/v2/everything?q=AAPL&apiKey={news_api_key}'
        response = requests.get(url)
        data = response.json()
        return data['articles']

    def store_stock_data(data):
        for timestamp, values in data.items():
            values['timestamp'] = timestamp
            engine.execute("""
                INSERT INTO stock_data (timestamp, open, high, low, close, volume) 
                VALUES (%(timestamp)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
                """, {
                    'timestamp': values['timestamp'],
                    'open': values['1. open'],
                    'high': values['2. high'],
                    'low': values['3. low'],
                    'close': values['4. close'],
                    'volume': values['5. volume']
                })

    def store_news_data(data):
        for article in data:
            engine.execute("""
                INSERT INTO news_data (title, description, content, published_at) 
                VALUES (%(title)s, %(description)s, %(content)s, %(published_at)s)
                """, {
                    'title': article['title'],
                    'description': article['description'],
                    'content': article['content'],
                    'published_at': article['publishedAt']
                })

    # Fetch and store data
    stock_data = fetch_stock_data()
    store_stock_data(stock_data)
    news_data = fetch_news_data()
    store_news_data(news_data)

if __name__ == "__main__":
    main()