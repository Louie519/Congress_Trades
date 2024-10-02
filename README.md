# Congress Trades Project

## Overview

The **Congress Trades Project** is an initiative to scrape stock trade data of U.S. congress members from the [House of Representatives Financial Disclosure website](https://disclosures-clerk.house.gov/FinancialDisclosure). While websites like CapitolTrades already aggregate this information, they do not provide downloadable CSVs, making it difficult to perform detailed data analysis. This project aims to create an ETL pipeline and a PostgreSQL database that captures stock trade information of U.S. politicians for further analysis.

## Motivation

The main goals of the project are:
- Scraping House members' stock trades from publicly available financial disclosures.
- Populating a PostgreSQL database with this information, including stock prices at the time of trade and future prices (50 and 100 days after).
- Maintaining an up-to-date record of trades by providing incremental updates to the database.
- Offering an easy way to analyze this data for financial insights.

## Database Schema

The database is designed to store detailed information about each trade, including transaction dates, stock prices, and details about the representative. Below is the table schema used in the database.

### Schema

Directory: `schema/congress_stock_trades_schema.sql`

```sql
CREATE TABLE congress_stock_trades (
    record_id SERIAL PRIMARY KEY,   -- Auto-incrementing primary key
    Year INT NOT NULL,              -- Year of the trade
    ID BIGINT,                      -- Unique identifier for the trade
    Representative VARCHAR(255),    -- Name of the representative
    District VARCHAR(10),           -- District code
    Transaction_Type CHAR(1),       -- Transaction type (P = Purchase, S = Sale)
    Ticker VARCHAR(10),             -- Stock ticker symbol
    Date DATE,                      -- Date of the transaction
    Notification_Date DATE,         -- Date the transaction was reported
    Amount NUMERIC(20, 6),          -- Amount involved in the trade
    Average_Price NUMERIC(20, 6),   -- Average stock price on transaction date
    Price_in_50_Days NUMERIC(20, 6),-- Stock price 50 days after the transaction
    Price_in_100_Days NUMERIC(20, 6),-- Stock price 100 days after the transaction
    Industry VARCHAR(255),          -- Industry of the stock
    Sector VARCHAR(255)             -- Sector of the stock
);
```
## ETL Scripts

There are three main scripts to create, populate, and maintain the database. Below are the descriptions for each file:

### 1. Initial Insert of Congress Trades

Directory: `initial_insert/congress_stock_trades_initial_insert.py`

This script performs the **initial data load** from the public financial disclosure site and inserts all congressperson stock trades up to the current date into the PostgreSQL database. The steps include:

- **Downloading and extracting** ZIP files containing the unique ids of financial disclosures from the House of Representatives website.
- **Parsing the disclosures** to extract details such as stock tickers, trade dates, and transaction amounts.
- **Using the `yfinance` library** to fetch stock prices at the time of the trade and 50 and 100 days after the trade.
- **Inserting the processed data** into the PostgreSQL database for future analysis.

[Link to the initial insert script](initial_insert/congress_stock_trades_initial_insert.py)

### 2. Incremental Updates to Insert New Trades

Directory: `incremental_update/congress_stock_trades_update.py`

This script performs **incremental updates** by fetching and inserting any new house member trades that have been disclosed since the initial insert. It ensures that the database stays up to date with new information. The steps include:

- **Querying the PostgreSQL database** to get the most recent document IDs for the current year.
- **Downloading and extracting** new disclosure files that were not part of the initial insert for the current year.
- **Processing and scraping** the necessary data from the new disclosure files that do not exist in the database, but exist in the updated record of reports.
- **Inserting the new trades** into the PostgreSQL database.

[Link to the incremental update script](incremental_update/congress_stock_trades_update.py)

### 3. Incremental Updates for Missing Future Stock Prices

Directory: `incremental_update/congress_stock_trades_stock_price_update.py`

This script handles **updating missing stock prices** for trades where future prices (50 days or 100 days after the trade) were not available at the time of the initial insert or trade update. The process includes:

- **Querying the database** to find trades that are missing `Price_in_50_Days` or `Price_in_100_Days`.
- **Using the `yfinance` library** to fetch the stock prices 50 and 100 days after the trade date.
- **Updating the PostgreSQL database** with the retrieved future prices.

[Link to the missing future prices update script](incremental_update/congress_stock_trades_stock_price_update.py)


# Setup Instructions

Follow these steps to set up the Congress Trades Project on your local machine:

### 1. Install PostgreSQL and Create a Database with the Schema

First, you need to install PostgreSQL and create a database for storing House member stock trades.

- Install PostgreSQL:
    - On Ubuntu: `sudo apt-get install postgresql postgresql-contrib`
    - On macOS (using Homebrew): `brew install postgresql`
    - On Windows: [Download and install PostgreSQL](https://www.postgresql.org/download/windows/).

- After installing PostgreSQL, create a new database:

```bash
# Access PostgreSQL CLI
psql -U your_username

# Create the database
CREATE DATABASE congress_trades;

# Connect to the newly created database
\c congress_trades;

# Create the table schema by running the SQL schema file
\i schema/congress_stock_trades_schema.sql
```
### 2. Fill in Your Database Information in the Initial Insert, Incremental Update, & Future Share Price Update files

Next, you need to provide your database connection details in the three scripts. Open the script and find the following section:

```python
conn = psycopg2.connect(
    host="YOUR_HOST",
    user="YOUR_USERNAME",
    password="YOUR_PASSWORD",
    database="congress_trades"
)
```
### 3. Populate the Database up to the most recent batch of disclosures.

```bash
#navigate to the directory where your clone is stored
cd ./initial_insert

#Run insert script with your db information
python3 congress_stock_trades_initial_insert.py
```

### 4. Periodically run congress_stock_trades_update.py & congress_stock_trades_stock_price_update.py to keep your db up to date.
```bash
#navigate to the directory where your clone is stored
cd ./incremental_update

#Run insert script with your db information
python3 congress_stock_trades_stock_price_update.py
```
# Future Goals

### Senator Trade Information

In the future these files will also aggregate Senator trade information. It is a more challenging task due to the fact that dynamically updated indices of recent trades are not available, requiring a Selenium based approach.


