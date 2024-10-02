import requests
import zipfile
import io
import pandas as pd
import re
import fitz
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import time
import psycopg2

# Convert transaction amount to float, handling string values
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

def merge_stock_data(df):
    grouped = df.groupby(['Ticker', 'Date']).apply(lambda x: get_prices(x['Ticker'].iloc[0], x['Date'].iloc[0]))
    grouped_df = pd.DataFrame(grouped.tolist(), index=grouped.index, columns=['Average_Price', 'Price_in_50_Days', 'Price_in_100_Days', 'Industry', 'Sector']).reset_index()
    return df.merge(grouped_df, on=['Ticker', 'Date'], how='left')

# Function to download and extract the text file from the ZIP archive
def download_and_extract_txt_file(zip_url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(zip_url, headers=headers)
        if response.status_code == 200:
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
            for file_name in zip_file.namelist():
                if file_name.endswith('.txt'):
                    with zip_file.open(file_name) as file:
                        return pd.read_csv(file, delimiter='\t')
        else:
            print(f"Failed to download file, status code: {response.status_code}")
            return None
    except zipfile.BadZipFile:
        print("The file is not a valid zip file.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Function to filter the document IDs that start with '2'
def get_valid_document_ids(df):
    df['DocID'] = df['DocID'].astype(str)  # Ensure DocID is treated as string
    valid_ids = df[df['DocID'].str.startswith('2')][['Year', 'DocID']]
    return list(valid_ids.itertuples(index=False, name=None))

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

# Clean the extracted text from the PDF
def clean_pdf_text(text):
    cleaned_text = re.sub(r'(?i)periodic.*?name:\n', '', text, flags=re.DOTALL)
    cleaned_text = re.sub(r'(?i)\nstatus.*?\nstate/district:', '|', cleaned_text, flags=re.DOTALL)
    cleaned_text = re.sub(r'(?i)\ntransactions.*?\namount\n', ' ', cleaned_text, flags=re.DOTALL)
    cleaned_text = re.sub(r'\n', ' ', cleaned_text, flags=re.DOTALL)
    cleaned_text = re.sub(r'(?s).*?name: ', '', cleaned_text, flags=re.IGNORECASE)
    cleaned_text = re.sub(r'\(partial\)', '', cleaned_text)  # Remove (partial)
    cleaned_text = re.sub(r'401\(K\)', '', cleaned_text)  # Remove 401(K)
    return cleaned_text

# Process cleaned text to extract the relevant fields
def process_cleaned_text(cleaned_text, year, unique_id):
    tickers = re.findall(r'\(([^)]+)\)', cleaned_text)
    transactions = re.split(r'\(\w+\)', cleaned_text)[1:]
    rep_name = re.search(r'^(.*?)\|', cleaned_text).group(1).strip() if re.search(r'^(.*?)\|', cleaned_text) else np.nan
    district = re.search(r'\|\s*(\S{4})', cleaned_text).group(1).strip() if re.search(r'\|\s*(\S{4})', cleaned_text) else np.nan
    data = []
    for ticker, details in zip(tickers, transactions):
        transaction_type = re.search(r'\b([PpSsEe])\b', details).group(1) if re.search(r'\b([PpSsEe])\b', details) else np.nan
        date, notification_date = extract_dates(details)
        amount = convert_amount(extract_amount(details))
        data.append([year, unique_id, rep_name, district, transaction_type, ticker, date, notification_date, amount])
    return data

def extract_dates(details):
    dates_match = re.findall(r'(\d{2}/\d{1,2}/\d{4})', details)
    if len(dates_match) >= 2:
        return dates_match[0], dates_match[1]
    elif len(dates_match) == 1:
        return dates_match[0], np.nan
    return np.nan, np.nan

def extract_amount(details):
    amount_match = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:,\d{3})*(?:\.\d{2})?)', details)
    return float(amount_match.group(1).replace(',', '')) if amount_match else np.nan

def batch_process_pdfs(valid_ids, batch_size=10):
    all_data = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for i in range(0, len(valid_ids), batch_size):
        batch = valid_ids[i:i + batch_size]
        for year, doc_id in batch:
            pdf_url = f'https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf'
            try:
                pdf_text = pdf_url_to_text(pdf_url, headers)
                cleaned_text = clean_pdf_text(pdf_text)
                all_data.extend(process_cleaned_text(cleaned_text, year, doc_id))
            except Exception as e:
                print(f"Error processing {doc_id}: {e}")
        time.sleep(1)  # Batch processing delay
    
    df = pd.DataFrame(all_data, columns=['Year', 'ID', 'Representative', 'District', 'Transaction_Type', 'Ticker', 'Date', 'Notification_Date', 'Amount'])
    return merge_stock_data(df)

#INSERT YOUR POSTGRES DB INFORMATION HERE
def insert_data_in_batches(data, batch_size=500):
    try:
        conn = psycopg2.connect(
            host="",
            user="",
            password="",  
            database=""
        )
        cursor = conn.cursor()

        query = """
        INSERT INTO congress_stock_trades 
        (Year, ID, Representative, District, Transaction_Type, Ticker, Date, Notification_Date, 
         Amount, Average_Price, Price_in_50_Days, Price_in_100_Days, Industry, Sector)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_tuples = [tuple(row) for row in batch.itertuples(index=False, name=None)]
            cursor.executemany(query, batch_tuples)
            conn.commit()

    except psycopg2.Error as err:
        print(f"Error: {err}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def clean_dataframe(df_data):
    df_data = df_data.dropna(subset=['Date', 'Amount'])  # Drop rows where critical fields are NaN
    df_data = df_data.dropna(subset=['Average_Price', 'Sector'], how='all') 
    df_data['Notification_Date'] = df_data['Notification_Date'].replace({np.nan: None})
    df_data['Transaction_Type'] = df_data['Transaction_Type'].str.upper()
    df_data['Ticker'] = df_data['Ticker'].str.upper()
    df_data = df_data[~df_data['Ticker'].isin(['PARTIAL', 'MERRILL LYNCH', 'NOT A SALE--THE CONGRESSIONAL PTR SYSTEM DOES NOT HAVE AN "OTHER" TRANSACTION TYPE.'])]  # Filter invalid tickers
    print(df_data.dtypes)
    return df_data

# Main function to process and insert data year by year
if __name__ == "__main__":
    for year in range(2014, 2025):
        print(f"Processing data for year {year}...")
        disclosure_url = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
        df = download_and_extract_txt_file(disclosure_url)

        if df is not None:
            print(f"Successfully downloaded data for {year}")
            valid_document_ids = get_valid_document_ids(df)
            df_data = batch_process_pdfs(valid_document_ids, batch_size=10)

            # Apply data cleaning
            df_data = clean_dataframe(df_data)

            # Insert into the database in batches
            print(f"Inserting data into the database for year {year}...")
            #insert_data_in_batches(df_data)

            # Reset variables to save memory
            del df_data
            del df
            print(f"Data for year {year} processed and inserted successfully.\n")
        else:
            print(f"Failed to retrieve document IDs for year {year}.")
