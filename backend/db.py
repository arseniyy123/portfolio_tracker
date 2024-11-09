import sqlite3

def create_tables(db_name):
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create stocks table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT UNIQUE,
            name TEXT
        )
    ''')
    
    # Create daily_prices table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id INTEGER,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            FOREIGN KEY (stock_id) REFERENCES stocks(id)
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
            FOREIGN KEY (stock_id) REFERENCES stocks(id)
        )
    ''')
    
    # Commit changes and close the connection
    conn.commit()
    conn.close()

# Example usage
if __name__ == "__main__":
    create_tables("portfolio_performance.db")
    print("Tables created successfully in 'portfolio_performance.db'")
