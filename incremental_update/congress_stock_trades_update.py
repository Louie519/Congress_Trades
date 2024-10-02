

import requests
import zipfile
import io
import psycopg2
import fitz  # PyMuPDF
import pandas as pd
import re
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta


# Database connection setup
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host="",
            user="",
            password="",  # Replace with your MySQL password
            database=""
        )
        return conn
    except psycopg2.Error as err:
        print(f"Error: {err}")
        return None


# Fetch the most recent document ID for the current year
def fetch_recent_entries(year):
    try:
        conn = connect_to_postgres()
        cursor = conn.cursor()
        query = f"""
        SELECT ID FROM congress_stock_trades 
        WHERE Year = {year}
        ORDER BY ID DESC
        """
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return {row[0] for row in result}  # Return set of existing IDs for the current year
    except psycopg2.Error as err:
        print(f"Error: {err}")
        return set()


# Download and extract the .txt file from the .zip archive
def download_and_extract_txt_file(zip_url):
    try:
        response = requests.get(zip_url)
        if response.status_code == 200:
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
            for file_name in zip_file.namelist():
                if file_name.endswith('.txt'):
                    with zip_file.open(file_name) as file:
                        return pd.read_csv(file, delimiter='\t')
        else:
            print(f"Failed to download file, status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# Filter document IDs that start with '2' and are not in the database
def get_new_document_ids(df, existing_ids):
    df['DocID'] = df['DocID'].astype(str)
    valid_ids = df[df['DocID'].str.startswith('2')][['Year', 'DocID']]
    return [(year, doc_id) for year, doc_id in valid_ids.itertuples(index=False) if doc_id not in existing_ids]


# Extract PDF text from URL
def pdf_url_to_text(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    pdf_document = fitz.open(stream=response.content, filetype="pdf")
    text = ""
    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)
        text += page.get_text()
    return text


# Clean the extracted PDF text
def clean_pdf_text(text):
    cleaned_text = re.sub(r'(?i)periodic.*?name:\n', '', text, flags=re.DOTALL)
    cleaned_text = re.sub(r'(?i)\nstatus.*?\nstate/district:', '|', cleaned_text, flags=re.DOTALL)
    cleaned_text = re.sub(r'(?i)\ntransactions.*?\namount\n', ' ', cleaned_text, flags=re.DOTALL)
    cleaned_text = re.sub(r'\n', ' ', cleaned_text, flags=re.DOTALL)
    cleaned_text = re.sub(r'(?s).*?name: ', '', cleaned_text, flags=re.IGNORECASE)
    cleaned_text = re.sub(r'\(partial\)', '', cleaned_text) 
    cleaned_text = re.sub(r'401\(K\)', '', cleaned_text) 
    return cleaned_text


# Extract fields from the cleaned PDF text
def process_cleaned_text(cleaned_text, year, unique_id):
    tickers = re.findall(r'\(([^)]+)\)', cleaned_text)
    transactions = re.split(r'\(\w+\)', cleaned_text)[1:]
    rep_name = re.search(r'^(.*?)\|', cleaned_text).group(1).strip() if re.search(r'^(.*?)\|', cleaned_text) else np.nan
    district = re.search(r'\|\s*(\S{4})', cleaned_text).group(1).strip() if re.search(r'\|\s*(\S{4})', cleaned_text) else np.nan
    data = []
    for ticker, details in zip(tickers, transactions):
        transaction_type = re.search(r'\b([PpSs])\b', details).group(1) if re.search(r'\b([PpSs])\b', details) else np.nan
        date, notification_date = extract_dates(details)
        amount = convert_amount(extract_amount(details))
        data.append([year, unique_id, rep_name, district, transaction_type, ticker, date, notification_date, amount])
    return data


# Extract dates from the transaction details
def extract_dates(details):
    dates_match = re.findall(r'(\d{2}/\d{1,2}/\d{4})', details)
    if len(dates_match) >= 2:
        return dates_match[0], dates_match[1]
    elif len(dates_match) == 1:
        return dates_match[0], np.nan
    return np.nan, np.nan


# Extract amount from transaction details
def extract_amount(details):
    amount_match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:,\d{3})*(?:\.\d{2})?)', details)
    return float(amount_match.group(1).replace(',', '')) if amount_match else np.nan


# Convert transaction amount
def convert_amount(amount):
    if isinstance(amount, (float, int)):
        return amount
    if isinstance(amount, str):
        amount = amount.replace('$', '').replace(',', '')
        try:
            return float(amount)
        except ValueError:
            return np.nan
    return np.nan


def get_prices(ticker, date):
    if pd.isna(date) or pd.isna(ticker):
        return None, None, None, None, None

    try:
        # Parse date from 'MM/DD/YYYY' to 'YYYY-MM-DD'
        date_str = datetime.strptime(str(date), '%m/%d/%Y').strftime('%Y-%m-%d')
        stock = yf.Ticker(ticker)
        industry, sector = stock.info.get('industry'), stock.info.get('sector')

        # Fetch the price at the given date using the same function as for future prices (days_ahead = 0)
        avg_price = find_future_price(stock, datetime.strptime(date_str, '%Y-%m-%d'), 0)
        avg_price_50 = find_future_price(stock, datetime.strptime(date_str, '%Y-%m-%d'), 50)
        avg_price_100 = find_future_price(stock, datetime.strptime(date_str, '%Y-%m-%d'), 100)

        # Round prices if they exist
        avg_price = avg_price.round(3) if avg_price is not None else None
        avg_price_50 = avg_price_50.round(3) if avg_price_50 is not None else None
        avg_price_100 = avg_price_100.round(3) if avg_price_100 is not None else None

        print(f"Ticker: {ticker}, Date: {date}, Avg Price: {avg_price}, Price in 50 Days: {avg_price_50}, Price in 100 Days: {avg_price_100}, Industry: {industry}, Sector: {sector}")
        return avg_price, avg_price_50, avg_price_100, industry, sector

    except Exception as e:
        print(f"Error retrieving prices for {ticker} on {date}: {e}")
        return None, None, None, None, None



def find_future_price(stock, start_date, days_ahead):
    next_date = start_date + timedelta(days_ahead)
    for _ in range(5):  # Retry mechanism
        hist = stock.history(start=next_date.strftime('%Y-%m-%d'), end=(next_date + timedelta(days=1)).strftime('%Y-%m-%d'))
        if not hist.empty:
            return hist['Close'].mean()
        next_date += timedelta(days=1)
    return None


# Merge stock data with the main DataFrame
def merge_stock_data(df):
    grouped = df.groupby(['Ticker', 'Date']).apply(lambda x: pd.Series(get_prices(x['Ticker'].iloc[0], x['Date'].iloc[0])))
    grouped_df = grouped.reset_index()
    grouped_df.columns = ['Ticker', 'Date', 'Average Price', 'Price in 50 Days', 'Price in 100 Days', 'Industry', 'Sector']
    return df.merge(grouped_df, on=['Ticker', 'Date'], how='left')


def insert_data_to_postgres(df):
    df = df.replace({np.nan: None})
    
    # Check the columns and rows for debugging
    print("Columns in DataFrame:", df.columns)
    print("Number of rows being inserted:", len(df))
    
    data = [tuple(row) for row in df.itertuples(index=False)]
    
    # Check the length of each tuple to ensure it matches the SQL query
    for idx, row in enumerate(data):
        if len(row) != 14:
            print(f"Row {idx} has {len(row)} elements: {row}")
        else:
            print(f"Row {idx} is valid")
    
    try:
        conn = connect_to_postgres()
        cursor = conn.cursor()
        query = """
        INSERT INTO congress_stock_trades 
        (Year, ID, Representative, District, Transaction_Type, Ticker, Date, Notification_Date, 
        Amount, Average_Price, Price_in_50_Days, Price_in_100_Days, Industry, Sector)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Execute batch insert
        cursor.executemany(query, data)
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} rows.")
        
    except psycopg2.Error as err:
        print(f"Error: {err}")
    
    finally:
        if conn:
            cursor.close()
            conn.close()



# Main function
if __name__ == "__main__":
    current_year = datetime.now().year

    # Step 1: Query the database for existing document IDs for the current year
    existing_ids = fetch_recent_entries(current_year)

    # Step 2: Download and extract the .zip file containing document IDs
    disclosure_url = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{current_year}FD.zip"
    df = download_and_extract_txt_file(disclosure_url)

    if df is not None:
        # Step 3: Get new document IDs not currently in the database
        new_document_ids = get_new_document_ids(df, existing_ids)

        # Step 4: Process PDFs for new document IDs and insert them into the database
        for year, doc_id in new_document_ids:
            pdf_url = f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf'
            try:
                pdf_text = pdf_url_to_text(pdf_url, {'User-Agent': 'Mozilla/5.0'})
                cleaned_text = clean_pdf_text(pdf_text)
                records = process_cleaned_text(cleaned_text, year, doc_id)
                df_data = pd.DataFrame(records, columns=['Year', 'ID', 'Representative', 'District', 'Transaction_Type','Ticker', 'Date', 'Notification Date', 'Amount'])
                df_data = merge_stock_data(df_data)
                
                df_data = df_data.dropna(subset=['Date', 'Amount']) 
                df_data = df_data.dropna(subset=['Average_Price', 'Sector'], how='all')
                df_data['Notification_Date'] = df_data['Notification_Date'].replace({np.nan: None})
                df_data = df_data[~df_data['Ticker'].isin(['PARTIAL', 'MERRILL LYNCH', 'NOT A SALE--THE CONGRESSIONAL PTR SYSTEM DOES NOT HAVE AN "OTHER" TRANSACTION TYPE.']) & (df_data['Ticker'].str.len() <= 10)]
                df_data = df_data[df_data['Transaction_Type'].str.len() == 1]
                df_data = df_data.drop(index=df_data[(df_data.Ticker =='PARTIAL')|(df_data.Ticker == 'MERRILL LYNCH')].index)
                df_data['Transaction_Type'] = df_data['Transaction_Type'].str.upper()
                df_data['Ticker'] = df_data['Ticker'].str.upper()
                
                insert_data_to_postgres(df_data)
            except Exception as e:
                print(f"Error processing {doc_id}: {e}")
                continue
    else:
        print("Failed to retrieve document IDs.")
