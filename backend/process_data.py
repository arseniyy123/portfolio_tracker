import pandas as pd
import math
import asyncio
from ticker_service import get_current_price, get_current_prices
import cProfile
import pstats
import io
from datetime import datetime


def clean_currency(value):
    """Convert currency strings to numeric values."""
    if isinstance(value, str):
        value = value.replace('USD', '').replace('EUR', '').replace(',', '').strip()
        try:
            return float(value) if value else 0
        except ValueError:
            print(f"Unable to convert value: {value}")
            return 0
    return float(value)

def parse_transaction_description(description, currency, tipo):
    # Example description: "Compra 2 Visa Inc@278,5 USD"
    description_parts = description.split('@')
    quantity = float(description_parts[0].split()[1])
    price = float(description_parts[1].split()[0].replace('.', '').replace(',', '.').split('@')[-1])
    if currency == 'USD':
        price = price / tipo  # Convert USD to EUR
    return quantity, price

async def calculate_profits_async(df):
    df = df[~df['ID Orden'].isna()]
    df = df[['Fecha', 'Producto', 'Descripción', 'Tipo', 'Variación', 'Saldo']]
    df_eur = df[df['Variación'] == 'EUR']
    df_usd = df[df['Variación'] == 'USD'].copy()
    df_usd.loc[:, 'Tipo'] = df_usd['Tipo'].fillna(method='ffill')
    df = pd.concat([df_eur, df_usd])

    # Initialize positions dictionary
    positions = {}
    profit_loss = 0.0
    profit_loss_breakdown = {}

    # Iterate over transactions
    for _, row in df[::-1].iterrows():
        product = row['Producto']
        description = row['Descripción']
        currency = row['Variación']

        if 'Compra' in description:
            # Buying shares
            quantity, price = parse_transaction_description(description, currency, row['Tipo'])
            if product not in positions:
                positions[product] = []
            positions[product].append({'quantity': quantity, 'cost_per_unit': math.ceil(price*100)/100})

        elif 'Venta' in description:
            # Selling shares
            quantity, price = parse_transaction_description(description, currency, row['Tipo'])
            if product in positions:
                remaining_quantity = quantity
                while remaining_quantity > 0 and positions[product]:
                    lot = positions[product][0]
                    if lot['quantity'] <= remaining_quantity:
                        profit_loss += (price - lot['cost_per_unit']) * lot['quantity']
                        if product not in profit_loss_breakdown:
                            profit_loss_breakdown[product] = []
                        profit_loss_breakdown[product].append({
                            'quantity': lot['quantity'],
                            'profit_per_unit': price - lot['cost_per_unit']
                        })
                        remaining_quantity -= lot['quantity']
                        positions[product].pop(0)
                    else:
                        profit_loss += (price - lot['cost_per_unit']) * remaining_quantity
                        if product not in profit_loss_breakdown:
                            profit_loss_breakdown[product] = []
                        profit_loss_breakdown[product].append({
                            'quantity': remaining_quantity,
                            'profit_per_unit': price - lot['cost_per_unit']
                        })
                        lot['quantity'] -= remaining_quantity
                        remaining_quantity = 0

    products_to_fetch = [product for product, lots in positions.items() if lots]

    # Fetch all current prices concurrently
    current_prices = await get_current_prices(products_to_fetch, 'EUR')
    current_prices = {product.lower(): price for product, price in current_prices.items()}
    
    # Calculate ongoing positions profit/loss
    for product, lots in positions.items():
        if lots:
            current_price = current_prices.get(product.lower(), 0.0)
            for lot in lots:
                profit_loss += (current_price - lot['cost_per_unit']) * lot['quantity']
                if product not in profit_loss_breakdown:
                    profit_loss_breakdown[product] = []
                profit_loss_breakdown[product].append({
                    'quantity': lot['quantity'],
                    'profit_per_unit': current_price - lot['cost_per_unit']
                })

    return round(profit_loss, 2), profit_loss_breakdown

async def calculate_metrics_async(account_df: pd.DataFrame, portfolio_df: pd.DataFrame) -> dict:
    profiler = cProfile.Profile()
    profiler.enable()

    # Prepare necessary columns
    amount_column = 'Unnamed: 8'  # Column containing amounts
    description_column = 'Descripción'

    # Ensure description column values are strings for proper filtering
    account_df[description_column] = account_df[description_column].astype(str)
    account_df[amount_column] = account_df[amount_column].apply(clean_currency)

    total_dividends_received = 0

    # Filter rows by 'Descripción' and 'ID Orden'
    filtered_df = account_df[
        account_df['Descripción'].isin([
            'Ingreso Cambio de Divisa',
            'Retirada Cambio de Divisa',
            'Dividendo',
            'Retención del dividendo'
        ]) & account_df['ID Orden'].isna()
    ]

    # Filter for relevant descriptions: Dividendo, Retención del dividendo, Retirada Cambio de Divisa
    relevant_descriptions = ['Dividendo', 'Retención del dividendo', 'Retirada Cambio de Divisa']
    relevant_df = filtered_df[filtered_df['Descripción'].isin(relevant_descriptions)].copy()

    relevant_df_eur = relevant_df[relevant_df['Saldo'] == 'EUR']
    relevant_df_eur = relevant_df_eur[~relevant_df_eur['Producto'].isna()]

    relevant_df_usd = relevant_df[relevant_df['Saldo'] == 'USD'].copy()
    relevant_df_usd.loc[:, 'Tipo'] = relevant_df_usd['Tipo'].fillna(method='ffill')

    # Create a new DataFrame to store the calculated results
    result_df = []

    # Process EUR dividends
    euro_groups = relevant_df_eur.groupby(['Fecha valor', 'Producto'])
    for group_id, group in euro_groups:
        dividend_row = group[group['Descripción'] == 'Dividendo']
        retention_row = group[group['Descripción'] == 'Retención del dividendo']

        # Skip if the dividend row is empty
        if dividend_row.empty:
            continue

        # Extract values for calculation
        dividend_amount = float(dividend_row[amount_column].iloc[0]) if not dividend_row.empty else 0
        retention_amount = abs(float(retention_row[amount_column].iloc[0])) if not retention_row.empty else 0

        # Calculate the net dividend
        net_dividend = dividend_amount - retention_amount

        # Store the result
        result_df.append({
            'Product': dividend_row['Producto'].iloc[0],
            'Gross Dividend': dividend_amount,
            'Retention': retention_amount,
            'Net Dividend': net_dividend,
            'Currency': 'EUR'
        })

    # Process USD dividends
    usd_groups = relevant_df_usd.groupby(['Fecha valor', 'Producto'])
    for group_id, group in usd_groups:
        dividend_row = group[group['Descripción'] == 'Dividendo']
        retention_row = group[group['Descripción'] == 'Retención del dividendo']
        conversion_row = group[group['Descripción'] == 'Retirada Cambio de Divisa']

        # Skip if the dividend row is empty
        if dividend_row.empty:
            continue

        # Extract values for calculation
        dividend_amount = float(dividend_row[amount_column].iloc[0]) if not dividend_row.empty else 0
        retention_amount = abs(float(retention_row[amount_column].iloc[0])) if not retention_row.empty else 0
        conversion_rate = float(conversion_row['Tipo'].iloc[0]) if not conversion_row.empty else 1

        # Calculate the net dividend and convert to EUR
        net_dividend = (dividend_amount - retention_amount) * conversion_rate

        # Store the result
        result_df.append({
            'Product': dividend_row['Producto'].iloc[0],
            'Gross Dividend': dividend_amount,
            'Retention': retention_amount,
            'Net Dividend': net_dividend,
            'Currency': 'EUR'
        })

    # Convert the result to a DataFrame
    result_df = pd.DataFrame(result_df)

    # Calculate the total dividends in EUR
    total_dividends_received = result_df['Net Dividend'].sum()

    # Step 2: Total Fees (Commissions, Taxes, etc.) Calculation with breakdown by type
    fees = account_df[account_df[description_column].str.contains("comisión|impuesto|tarifa|coste|fee|connection|FTT", case=False, na=False)]
    fees = fees.copy()
    fees[amount_column] = fees[amount_column].apply(clean_currency)

    # Categorize fees
    fee_categories = {
        'Transaction Fees': ["transaction", "coste"],
        'Exchange Fees': ["connection", "exchange", "conectividad"],
        'FTT Fees': ["impuesto", "FTT", "financial transaction tax"],
        'ADR/GDR Fees': ["ADR", "GDR", "pass-through"]
    }

    fee_summary = {key: 0 for key in fee_categories}

    # Categorize and sum the fees
    for idx, row in fees.iterrows():
        for category, keywords in fee_categories.items():
            if any(keyword.lower() in row[description_column].lower() for keyword in keywords):
                fee_summary[category] += row[amount_column]
                break

    # Round fee summary values to 2 decimals
    fee_summary = {key: round(value, 2) for key, value in fee_summary.items()}
    total_fees = round(fees[amount_column].sum(), 2)

    # Step 3: Profit/Loss Calculation for Each Company Using Account Data
    profit_loss, profit_loss_breakdown = await calculate_profits_async(account_df)

    # Step 4: Portfolio Balance and Cash Calculation using 
    portfolio_df['Valor en EUR'] = portfolio_df['Valor en EUR'].apply(lambda x: float(x.replace(',', '.')))
    cash = portfolio_df[portfolio_df['Producto'].str.contains("cash", case = False, na=False)]['Valor en EUR'].sum()
    portfolio_value = round(portfolio_df['Valor en EUR'].sum() - cash, 2)

    # Step 5: Returns
    account_df = account_df[account_df['Fecha'].notna()]
    historical_portfolio_value = []  # Generate time-series data for portfolio value
    historical_cashflow = []  # Generate time-series data for cash flow

    # Calculate cumulative cash flow over time
    cumulative_cashflow = 0
    for _, row in account_df[::-1].iterrows():
        try:
            if row['Variación'] == 'EUR':
                cumulative_cashflow += row['Unnamed: 8']
            historical_cashflow.append({
                'date': str(datetime.strptime(row['Fecha'], '%d-%m-%Y')),
                'value': cumulative_cashflow
            })
        except Exception as e:
            print(f"Unable to parse date: {row['Fecha']}, error: {e}")

    historical_portfolio_value = historical_cashflow
        


    # Calculate annual growth rate
    initial_value = historical_portfolio_value[0]['value'] if historical_portfolio_value else 1
    final_value = historical_portfolio_value[-1]['value'] if historical_portfolio_value else 1
    number_of_years = (datetime.now() - min(account_df['Fecha'].apply(lambda x: datetime.strptime(x, '%d-%m-%Y')))).days / 365.25
    annual_growth_rate = ((final_value / initial_value) ** (1 / number_of_years) - 1) * 100 if number_of_years > 0 else 0


    # Stop the profiler
    profiler.disable()

    # Create a stream to capture the profiling stats
    s = io.StringIO()
    sortby = 'cumtime'
    ps = pstats.Stats(profiler, stream=s).sort_stats(sortby)
    ps.print_stats(10)  # Limit output to top 10 functions
    print(s.getvalue())


    # Return results
    return {
        "total_dividends": total_dividends_received,
        "total_fees": total_fees,
        "fee_breakdown": fee_summary,
        "profit_loss": profit_loss,
        "profit_loss_breakdown": profit_loss_breakdown,
        "portfolio_value": portfolio_value,
        "cash_balance": cash,
        "historical_portfolio_value": historical_portfolio_value,
        "historical_cashflow": historical_cashflow,
        "annual_growth_rate": round(annual_growth_rate, 2)
    }