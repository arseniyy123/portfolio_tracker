import yfinance as yf
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, date


# def get_stock_data(symbol: str, start='2010-01-01'):
#     """
#     Fetches historical stock data since 2010 for a given company symbol using yfinance.
#     """
#     try:
#         ticker = yf.Ticker(symbol)
#         data = ticker.history(start=start)
#         data.reset_index(inplace=True)
#         data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')  # Format Date as 'YYYY-MM-DD'
#         data['Ticker'] = symbol
#         return data[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
#     except Exception as e:
#         print(f"Error fetching data for {symbol}: {e}")
#         return None


def get_stock_data(symbol: str, start='2010-01-01'):
    """
    Fetches historical stock data since 2010 for a given company symbol using yfinance.
    Adjusts for stock splits.
    """
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(start=start)
        data.reset_index(inplace=True)
        data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')  # Format Date as 'YYYY-MM-DD'
        data['Ticker'] = symbol

        # Get cumulative product of split ratios (reverse cumulative product)
        data['Split Ratio'] = data['Stock Splits'].replace(0, 1)
        data['Cumulative Split Ratio'] = data['Split Ratio'][::-1].cumprod()[::-1]

        # Adjust prices for splits
        data['Open'] = data['Open'] * data['Cumulative Split Ratio']
        data['High'] = data['High'] * data['Cumulative Split Ratio']
        data['Low'] = data['Low'] * data['Cumulative Split Ratio']
        data['Close'] = data['Close'] * data['Cumulative Split Ratio']
        data.columns = data.columns.str.replace(' ', '_')

        # Return adjusted prices
        return data[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_Splits']]
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None


def update_stock_data_table(symbols, db_path='stocks.db'):
    """
    Updates the stock data table for the given list of symbols in an SQLite database.
    Only fetches and adds missing or outdated data.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for symbol in symbols:
        # Check the latest available date for the symbol in the database
        cursor.execute('''
            SELECT MAX(Date) FROM stock_data WHERE Ticker = ?
        ''', (symbol,))
        result = cursor.fetchone()
        last_date_in_db = result[0]

        # If no data is in the database, fetch all data since 2010
        if last_date_in_db is None:
            print(f"No data found for {symbol}, fetching all data since 2010.")
            stock_data = get_stock_data(symbol)
        else:
            # Otherwise, fetch data from the last date up to today if it’s outdated
            last_date_in_db_datetime = datetime.strptime(last_date_in_db, "%Y-%m-%d")
            now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            if last_date_in_db_datetime < now and now.weekday() < 5:
                print(f"Updating data for {symbol} from {last_date_in_db_datetime + pd.Timedelta(days=1)} to today.")
                stock_data = get_stock_data(symbol)
                stock_data = stock_data[stock_data['Date'] > last_date_in_db]
            else:
                print(f"Data for {symbol} is already up-to-date.")
                stock_data = None

        # If there's new or missing data, insert it into the database
        if stock_data is not None and not stock_data.empty:
            stock_data.to_sql('stock_data', conn, if_exists='append', index=False)
            print(f"Inserted {len(stock_data)} new rows for {symbol}.")

    conn.commit()
    conn.close()
    print("Stock data update complete.")


def calculate_daily_profit_loss(positions, products_to_fetch, db_path='stocks.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    exchange_rates = load_exchange_rates(db_path)

    daily_profits = {}  # Dictionary to store daily profit/loss for each stock and day

    for company, lots in positions.items():
        ticker = products_to_fetch.get(company)

        if not ticker:
            print(f"No ticker found for {company}")
            continue

        for lot in lots:
            quantity = lot['quantity']
            cost_per_unit = lot['cost_per_unit']
            start_date = lot['start_date']
            end_date = lot['end_date'] if lot['end_date'] else datetime.now()
            currency = lot.get('currency', 'USD')

            # Query stock_data for prices between start_date and end_date
            cursor.execute('''
                SELECT Date, Close FROM stock_data 
                WHERE Ticker = ? AND Date BETWEEN ? AND ?
                ORDER BY Date
            ''', (ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))

            daily_prices = cursor.fetchall()

            for date_str, close_price in daily_prices:
                date = datetime.strptime(date_str, '%Y-%m-%d')
                if currency == 'USD':
                    # Get the EUR/USD exchange rate for the specific date
                    exchange_rate = exchange_rates.get(date)
                    if exchange_rate:
                        # Convert close price from USD to EUR
                        close_price = close_price / exchange_rate

                # Calculate daily profit/loss
                daily_profit_loss = (close_price - cost_per_unit) * quantity

                # Store daily profit/loss in dictionary
                if company not in daily_profits:
                    daily_profits[company] = {}

                daily_profits[company][date] = daily_profits[company].get(date, 0) + daily_profit_loss

    conn.close()
    return daily_profits


def load_exchange_rates(db_path='stocks.db'):
    """
    Load all EUR/USD exchange rates from the database into a dictionary.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Load all exchange rates into a dictionary
    cursor.execute("SELECT date, exchange_rate FROM eur_usd_exchange")
    exchange_rates = {datetime.strptime(row[0], '%Y-%m-%d'): row[1] for row in cursor.fetchall()}

    conn.close()
    return exchange_rates

def calculate_total_daily_profit_loss(positions, products_to_fetch, db_path='stocks.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Dictionary to store overall daily profit/loss across all companies
    total_daily_profits = {}
    exchange_rates = load_exchange_rates(db_path)

    for company, lots in positions.items():
        ticker = products_to_fetch.get(company)

        if not ticker:
            print(f"No ticker found for {company}")
            continue

        for lot in lots:
            quantity = lot['quantity']
            cost_per_unit = lot['cost_per_unit']
            start_date = lot['start_date']
            end_date = lot['end_date'] if lot['end_date'] else datetime.now()
            currency = lot.get('currency', 'USD')

            # Query stock_data for prices between start_date and end_date
            cursor.execute('''
                SELECT Date, Close FROM stock_data 
                WHERE Ticker = ? AND Date BETWEEN ? AND ?
                ORDER BY Date
            ''', (ticker, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))

            daily_prices = cursor.fetchall()

            for date_str, close_price in daily_prices:
                date_ = datetime.strptime(date_str, '%Y-%m-%d')
                if currency == 'USD':
                    # Get the EUR/USD exchange rate for the specific date
                    exchange_rate = exchange_rates.get(date_)
                    if exchange_rate:
                        # Convert close price from USD to EUR
                        close_price = close_price / exchange_rate
                # Calculate daily profit/loss for this lot
                daily_profit_loss = (close_price - cost_per_unit) * quantity

                # Aggregate the daily profit/loss at the total level
                if date_ not in total_daily_profits:
                    total_daily_profits[date_] = 0
                total_daily_profits[date_] += daily_profit_loss

    conn.close()

    # Sort the results by date for easier reading
    sorted_total_daily_profits = dict(sorted(total_daily_profits.items()))
    return sorted_total_daily_profits


def update_exchange_rate_data(db_path='stocks.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check the latest date in the database
    cursor.execute("SELECT MAX(date) FROM eur_usd_exchange")
    result = cursor.fetchone()
    last_date_in_db = result[0]

    # Determine start date for fetching data
    if last_date_in_db is None:
        # If no data is present, fetch from 2010-01-01
        start_date = "2010-01-01"
    else:
        # Fetch data from the day after the last recorded date
        start_date = (datetime.strptime(last_date_in_db, "%Y-%m-%d") + timedelta(days=1)).strftime('%Y-%m-%d')

    # Fetch data from start_date to today
    eur_usd_data = yf.Ticker("EURUSD=X").history(start=start_date)
    eur_usd_data.reset_index(inplace=True)
    eur_usd_data['Date'] = eur_usd_data['Date'].dt.strftime('%Y-%m-%d')  # Format date as 'YYYY-MM-DD'

    # Insert new data into the database
    for _, row in eur_usd_data.iterrows():
        date = row['Date']
        exchange_rate = row['Close']

        # Insert only if the date does not already exist
        cursor.execute("SELECT 1 FROM eur_usd_exchange WHERE date = ?", (date,))
        if cursor.fetchone() is None:
            cursor.execute('''
                INSERT INTO eur_usd_exchange (date, exchange_rate, date_added)
                VALUES (?, ?, ?)
            ''', (date, exchange_rate, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    conn.commit()
    conn.close()
    print("EUR/USD exchange rate data updated successfully.")
