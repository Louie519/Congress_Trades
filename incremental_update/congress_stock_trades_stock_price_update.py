import psycopg2
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

# Connect to PostgreSQL database
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host="",
            user="",
            password="",  # Replace with your PostgreSQL credentials
            database=""
        )
        return conn
    except psycopg2.Error as err:
        print(f"Error: {err}")
        return None

# Query the database for missing price entries
def fetch_transactions_to_update():
    try:
        conn = connect_to_postgres()
        cursor = conn.cursor()
        # Get the records from the last 200 days where Price_in_50_Days or Price_in_100_Days are missing
        query = """
        SELECT record_id, Ticker, Date
        FROM congress_stock_trades
        WHERE Date >= CURRENT_DATE - INTERVAL '200 days'
        AND (Price_in_50_Days ='NaN' OR Price_in_100_Days ='NaN')
        """
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except psycopg2.Error as err:
        print(f"Error fetching transactions: {err}")
        return []

# Update the stock prices in the database
def update_stock_prices(record_id, price_50_days, price_100_days):
    try:
        conn = connect_to_postgres()
        cursor = conn.cursor()
        # Only update the fields that are missing (NULL)
        query = """
        UPDATE congress_stock_trades
        SET Price_in_50_Days = COALESCE(Price_in_50_Days, %s),
            Price_in_100_Days = COALESCE(Price_in_100_Days, %s)
        WHERE record_id = %s
        """
        cursor.execute(query, (price_50_days, price_100_days, record_id))
        conn.commit()
        cursor.close()
        conn.close()
    except psycopg2.Error as err:
        print(f"Error updating record {record_id}: {err}")

# Retrieve stock prices for a given ticker and date
def get_prices(ticker, date):
    if pd.isna(date) or pd.isna(ticker):
        return None, None
    date_str = datetime.strptime(str(date), '%Y-%m-%d').strftime('%Y-%m-%d')
    stock = yf.Ticker(ticker)
    try:
        hist = stock.history(start=date_str, end=(datetime.strptime(date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d'))
        avg_price = hist['Close'].mean() if not hist.empty else None
        avg_price_50 = find_future_price(stock, datetime.strptime(date_str, '%Y-%m-%d'), 50)
        avg_price_100 = find_future_price(stock, datetime.strptime(date_str, '%Y-%m-%d'), 100)
        return avg_price_50, avg_price_100
    except Exception as e:
        print(f"Error retrieving prices for {ticker} on {date}: {e}")
        return None, None

# Find future stock prices after the given number of days
def find_future_price(stock, start_date, days_ahead):
    next_date = start_date + timedelta(days_ahead)
    for _ in range(5):
        hist = stock.history(start=next_date.strftime('%Y-%m-%d'), end=(next_date + timedelta(days=1)).strftime('%Y-%m-%d'))
        if not hist.empty:
            return hist['Close'].mean()
        next_date += timedelta(days=1)
    return None

# Main function to update missing prices
if __name__ == "__main__":
    transactions = fetch_transactions_to_update()

    for record in transactions:
        record_id, ticker, date = record

        # Check if the transaction date is exactly 50 or 100 days ago
        today = datetime.now().date()
        delta_50_days = today - timedelta(days=50)
        delta_100_days = today - timedelta(days=100)

        price_50_days, price_100_days = None, None

        if date == delta_50_days or date == delta_100_days:
            price_50_days, price_100_days = get_prices(ticker, date)

        # Update the database if prices were found
        if price_50_days or price_100_days:
            update_stock_prices(record_id, price_50_days, price_100_days)
        else:
            print(f"No update needed for record {record_id}, trade on {date}")
