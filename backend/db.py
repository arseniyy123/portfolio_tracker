import sqlite3

# Database setup
def create_tables(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create a table for storing ticker information
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tickers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product TEXT,
            ticker TEXT,
            date_added TEXT
        )
    ''')

    # Create profit_loss table
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS eur_usd_exchange (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE,
                exchange_rate REAL,
                date_added TEXT
            )
        ''')

    # Create stock_data table
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_data (
                Date TEXT,
                Ticker TEXT,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Volume INTEGER,
                Dividends REAL,
                Stock_Splits REAL,
                PRIMARY KEY (Date, Ticker)
            )
        ''')

    # Create portfolio table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            quantity REAL,
            purchase_date TEXT,
            purchase_price REAL,
            FOREIGN KEY (stock_id) REFERENCES tickers(id)
        )
    ''')

    # Create profit_loss table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS profit_loss (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            date TEXT,
            profit_loss REAL,
            FOREIGN KEY (stock_id) REFERENCES tickers(id)
        )
    ''')

    conn.commit()
    conn.close()