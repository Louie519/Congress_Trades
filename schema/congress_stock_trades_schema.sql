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
