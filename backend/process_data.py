import pandas as pd
from ticker_service import get_processed_tickers, get_ticker_symbol
from stock_service import update_stock_data_table, calculate_total_daily_profit_loss
import cProfile
import pstats
import io
from datetime import datetime
import holidays


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
    df_usd.loc[:, 'Tipo'] = df_usd['Tipo'].ffill()
    df = pd.concat([df_eur, df_usd])

    # Initialize positions dictionary to track stocks and their purchase data
    positions = {}

    # Iterate over transactions to build positions and get tickers
    for _, row in df[::-1].iterrows():
        product = row['Producto']
        description = row['Descripción']
        currency = row['Variación']
        date = datetime.strptime(row['Fecha'], '%d-%m-%Y')

        if 'Compra' in description:
            # Buying shares
            quantity, price = parse_transaction_description(description, currency, row['Tipo'])
            if product not in positions:
                positions[product] = []
            positions[product].append(
                {'currency': currency, 'quantity': quantity, 'cost_per_unit': price, 'start_date': date,
                 'end_date': None})

        elif 'Venta' in description:
            # Selling shares
            quantity, price = parse_transaction_description(description, currency, row['Tipo'])
            if product in positions:
                lot_num = 0
                remaining_quantity = quantity
                while remaining_quantity > 0:
                    lot = positions[product][lot_num]
                    if lot['quantity'] <= remaining_quantity:
                        remaining_quantity -= lot['quantity']
                        lot['end_date'] = date
                        # positions[product].pop(0)
                    else:
                        lot['quantity'] -= remaining_quantity
                        lot['end_date'] = date
                        remaining_quantity = 0
                    lot_num += 1
    # Get already processed tickers
    products_to_fetch = get_processed_tickers(positions.keys())

    # Get tickers for products that are not already processed
    for product, value in products_to_fetch.items():
        if value == 'NA':
            products_to_fetch[product] = await get_ticker_symbol(
                product.lower().replace('adr on ', '').replace('class c', '').replace('class a', '').replace(
                    'class b', '').replace('.com', '').strip())

    # Update stock data table with new data
    update_stock_data_table(products_to_fetch.values())

    # daily_profits = calculate_daily_profit_loss(positions, products_to_fetch)
    # Get overall daily profit/loss
    overall_daily_profits = calculate_total_daily_profit_loss(positions, products_to_fetch)

    us_holidays = holidays.US(years=range(2010, 2030))
    filtered_data = {
        date: round(value, 2)
        for date, value in overall_daily_profits.items()
        if date.weekday() < 5  # Exclude weekends
           and date not in us_holidays  # Exclude US public holidays
    }

    filtered_data = pd.DataFrame(list(filtered_data.items()), columns=['date', 'value'])

    return filtered_data

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
    relevant_df_usd.loc[:, 'Tipo'] = relevant_df_usd['Tipo'].ffill()

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
    #profit_loss, profit_loss_breakdown = await calculate_profits_async(account_df)
    historical_portfolio_value = await calculate_profits_async(account_df)
    profit_loss = round(float(historical_portfolio_value['value'].iloc[-1]), 2)

    # Step 4: Portfolio Balance and Cash Calculation using
    portfolio_df['Valor en EUR'] = pd.to_numeric(portfolio_df['Valor en EUR'].str.replace(',', '.'), errors='coerce')
    cash = portfolio_df[portfolio_df['Producto'].str.contains("cash", case = False, na=False)]['Valor en EUR'].sum()
    portfolio_value = round(portfolio_df['Valor en EUR'].sum() - cash, 2)

    # Step 5: Returns
    account_df = account_df[account_df['Fecha'].notna()]
    historical_cashflow = []  # Generate time-series data for cash flow

    # Calculate cumulative cash flow over time

    cumulative_cashflow = account_df[account_df['Variación'] == 'EUR'][amount_column][::-1].cumsum()
    historical_cashflow = pd.DataFrame({
        'date': account_df['Fecha'][::-1],
        'value': cumulative_cashflow
    }).ffill()

    # cumulative_cashflow = 0
    # for _, row in account_df[::-1].iterrows():
    #     try:
    #         if row['Variación'] == 'EUR':
    #             cumulative_cashflow += row['Unnamed: 8']
    #         historical_cashflow.append({
    #             'date': row['Fecha'],
    #             'value': cumulative_cashflow
    #         })
    #     except Exception as e:
    #         print(f"Unable to parse date: {row['Fecha']}, error: {e}")
    #
    # historical_cashflow = pd.DataFrame(historical_cashflow)
    historical_cashflow['value'] = historical_cashflow['value'].round(2)

    historical_portfolio_value['date'] = historical_portfolio_value['date'].dt.strftime('%Y-%m-%d')

    # Convert 'date' column to datetime format
    historical_cashflow['date'] = pd.to_datetime(historical_cashflow['date'], format='%d-%m-%Y')
    # Remove duplicate dates by keeping the last occurrence
    historical_cashflow = historical_cashflow.drop_duplicates(subset='date', keep='last')
    # Create a complete date range and reindex the DataFrame to fill missing dates
    all_dates = pd.date_range(historical_cashflow['date'].min(), historical_portfolio_value['date'].max(), freq='D')
    historical_cashflow = historical_cashflow.set_index('date').reindex(all_dates).ffill().bfill().reset_index()
    historical_cashflow.columns = ['date', 'value']
    # convert date in string Year Month Day format
    historical_cashflow['date'] = historical_cashflow['date'].dt.strftime('%Y-%m-%d')

    historical_portfolio_value['date'] = pd.to_datetime(historical_portfolio_value['date'], format='%Y-%m-%d')
    historical_portfolio_value = historical_portfolio_value.drop_duplicates(subset='date', keep='last')
    historical_portfolio_value = historical_portfolio_value.set_index('date').reindex(all_dates).ffill().bfill().reset_index().rename(columns={'index': 'date'})
    historical_portfolio_value['date'] = historical_portfolio_value['date'].dt.strftime('%Y-%m-%d')

    # Merge DataFrames on 'date'
    merged_df = pd.merge(historical_cashflow, historical_portfolio_value, on='date', how='right').ffill()
    merged_df.rename(columns={'value_x': 'cashflow_value', 'value_y': 'portfolio_value'}, inplace=True)

    # Sum the 'value' fields
    merged_df['value'] = merged_df['cashflow_value'] + merged_df['portfolio_value']

    combined_data = merged_df[['date', 'value']].copy()

    combined_data['value'] = pd.to_numeric(combined_data['value'], errors='coerce')
    combined_data['value'] = combined_data['value'].round(2)
    historical_portfolio_value['value'] = pd.to_numeric(historical_portfolio_value['value'], errors='coerce')
    historical_portfolio_value['value'] = historical_portfolio_value['value'].round(2)
    historical_cashflow['value'] = pd.to_numeric(historical_cashflow['value'], errors='coerce')
    historical_cashflow['value'] = historical_cashflow['value'].round(2)

    combined_data = combined_data.to_dict(orient='records')
    historical_portfolio_value = historical_portfolio_value.to_dict(orient='records')
    historical_cashflow = historical_cashflow.to_dict(orient='records')

    # Calculate annual growth rate
    annual_growth_rate = 0
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
        #"profit_loss_breakdown": profit_loss_breakdown,
        "portfolio_value": portfolio_value,
        "cash_balance": cash,
        "historical_portfolio_value": historical_portfolio_value,
        "historical_cashflow": historical_cashflow,
        "combined_data": combined_data,
        "annual_growth_rate": round(annual_growth_rate, 2)
    }