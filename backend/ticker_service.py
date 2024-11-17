import os
import json
import yfinance as yf
from yahooquery import search
import aiohttp
import pandas as pd
from datetime import datetime
import asyncio
import sqlite3

# Load or initialize caches
TICKER_CACHE_FILE = "ticker_cache.json"
PRICE_CACHE_FILE = "price_cache.json"
USD_TO_EUR_CACHE_FILE = "usd_to_eur_cache.json"


def create_ticker_table(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create a table for storing ticker information
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tickers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product TEXT UNIQUE,
            ticker_symbol TEXT,
            date_added TEXT
        )
    """
    )

    conn.commit()
    conn.close()


# Call the function to create tickers table
create_ticker_table("portfolio_performance.db")

if os.path.exists(TICKER_CACHE_FILE):
    with open(TICKER_CACHE_FILE, "r") as f:
        ticker_cache = json.load(f)
else:
    ticker_cache = {}

if os.path.exists(PRICE_CACHE_FILE):
    with open(PRICE_CACHE_FILE, "r") as f:
        price_cache = json.load(f)
else:
    price_cache = {}

if os.path.exists(USD_TO_EUR_CACHE_FILE):
    with open(USD_TO_EUR_CACHE_FILE, "r") as f:
        usd_to_eur_cache = json.load(f)
else:
    usd_to_eur_cache = {}


async def get_ticker_symbol(product, db_path="stocks.db"):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if the ticker exists in the database
    cursor.execute("SELECT ticker FROM tickers WHERE product = ?", (product,))
    result = cursor.fetchone()

    if result:
        ticker = result[0]
        conn.close()
        return ticker

    url = f"https://query1.finance.yahoo.com/v1/finance/search?q={product}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                quotes = data.get("quotes", [])
                if quotes:
                    ticker = quotes[0]["symbol"]
                    if ticker:
                        # Insert ticker into the database
                        cursor.execute(
                            """
                            INSERT INTO tickers (product, ticker, date_added)
                            VALUES (?, ?, ?)
                        """,
                            (
                                product,
                                ticker,
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
                        conn.commit()
                    conn.close()
                    return ticker
    return ""


async def get_current_price(product, currency="USD"):
    # Check if price is already in cache and if it's from today
    today = datetime.now().strftime("%Y-%m-%d")
    if product in price_cache and price_cache[product]["date"] == today:
        return price_cache[product]["price"]

    # Get the current price using yfinance
    product = (
        product.lower()
        .replace("adr on ", "")
        .replace("class c", "")
        .replace("class a", "")
        .replace("class b", "")
        .replace(".com", "")
    )
    ticker = await get_ticker_symbol(product.strip())
    if not ticker:
        print(f"Warning: Could not find ticker for {product}")
        return 0.0

    async with aiohttp.ClientSession() as session:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                current_price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
                # Convert price to EUR if needed
                if (
                    currency == "EUR"
                    and data["chart"]["result"][0]["meta"]["currency"] != "EUR"
                ):
                    conversion_rate = get_usd_to_eur_rate()
                    current_price /= conversion_rate
                # Store the result in cache
                price_cache[product] = {"price": current_price, "date": today}
                # Save cache to disk
                with open(PRICE_CACHE_FILE, "w") as f:
                    json.dump(price_cache, f)
                return current_price
            else:
                print(f"Warning: Failed to fetch data for ticker {ticker}")
                return 0.0


def get_usd_to_eur_rate():
    today = datetime.now().strftime("%Y-%m-%d")
    if "rate" in usd_to_eur_cache and usd_to_eur_cache["date"] == today:
        return usd_to_eur_cache["rate"]

    # Get USD to EUR conversion rate using Yahoo Finance
    fx_ticker = yf.Ticker("EURUSD=X")
    current_rate = fx_ticker.history(period="1d")["Close"].iloc[-1]
    if pd.notna(current_rate):
        usd_to_eur_cache["rate"] = current_rate
        usd_to_eur_cache["date"] = today
        with open(USD_TO_EUR_CACHE_FILE, "w") as f:
            json.dump(usd_to_eur_cache, f)
        return current_rate

    print("Warning: Could not retrieve USD to EUR conversion rate")
    return 1.0


async def get_current_prices(products, currency="USD"):
    today = datetime.now().strftime("%Y-%m-%d")
    prices = {}

    # List of products that need fetching
    products_to_fetch = [
        product
        for product in products
        if product not in price_cache or price_cache[product]["date"] != today
    ]

    # Prepare tasks for products that need fetching
    tasks = []
    for product in products_to_fetch:
        tasks.append(fetch_price_for_product(product, currency))

    # Run tasks concurrently
    results = await asyncio.gather(*tasks)

    # Update caches and prepare the final prices dictionary
    for product, price in results:
        if price is not None:
            prices[product] = price
            price_cache[product] = {"price": price, "date": today}
            # Optionally, save cache to disk here or batch later

    # Include prices from the cache
    for product in products:
        if product in price_cache and price_cache[product]["date"] == today:
            prices[product] = price_cache[product]["price"]

    return prices


async def fetch_price_for_product(product, currency):
    # Fetch ticker symbol
    ticker = await get_ticker_symbol(
        product.lower()
        .replace("adr on ", "")
        .replace("class c", "")
        .replace("class a", "")
        .replace("class b", "")
        .replace(".com", "")
        .strip()
    )
    if not ticker:
        print(f"Warning: Could not find ticker for {product}")
        return product, None

    # Fetch current price
    async with aiohttp.ClientSession() as session:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                current_price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
                # Convert price to EUR if needed
                if (
                    currency == "EUR"
                    and data["chart"]["result"][0]["meta"]["currency"] != "EUR"
                ):
                    conversion_rate = get_usd_to_eur_rate()
                    current_price /= conversion_rate
                return product, current_price
            else:
                print(f"Warning: Failed to fetch data for ticker {ticker}")
                return product, None


async def get_historical_prices(ticker, start_date, end_date):
    # Use yfinance to get historical price data between start_date and end_date
    ticker_data = yf.Ticker(ticker)
    historical_data = ticker_data.history(
        start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d")
    )
    return historical_data


def get_processed_tickers(products, db_name="stocks.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    tickers = {}
    for product in products:
        cursor.execute(
            "SELECT ticker FROM tickers WHERE product = ?", (product.lower(),)
        )
        result = cursor.fetchone()
        if result:
            print(f"Found ticker {result[0]} for {product}")
            tickers[product] = result[0]
        else:
            tickers[product] = "NA"
    conn.close()
    return tickers


# TO BE IMPLEMENTED
def save_caches():
    with open(TICKER_CACHE_FILE, "w") as f:
        json.dump(ticker_cache, f)
    with open(PRICE_CACHE_FILE, "w") as f:
        json.dump(price_cache, f)
    with open(USD_TO_EUR_CACHE_FILE, "w") as f:
        json.dump(usd_to_eur_cache, f)
