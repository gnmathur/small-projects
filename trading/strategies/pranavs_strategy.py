import pandas as pd
import datetime
import os
import time
from pathlib import Path
import yfinance as yf
import numpy as np

"""
STOCK SCANNER FOR US MARKET

TRADING STRATEGY OVERVIEW:
This scanner implements a trend-following strategy with volume confirmation, based on:
1. Price relative to last year's high/low (trend direction)
2. Price relative to 200-period moving averages (long-term trend confirmation)
3. Volume analysis (smart money footprint confirmation)

BUY SIGNALS:
- Price > Last Year's Highest Price (breaking resistance)
- Price > 200-period Weekly SMA (long-term uptrend)
- Price > 200-period Daily SMA (medium-term uptrend)
- Volume confirmation: High volume on up days (accumulation)

SELL SIGNALS:
- Price < Last Year's Lowest Price (breaking support)
- Price < 200-period Weekly SMA (long-term downtrend)
- Price < 200-period Daily SMA (medium-term downtrend)
- Volume confirmation: High volume on down days (distribution)
"""

# Global lists for stocks meeting buy and sell criteria
buys = []
sells = []


def clear_screen():
    """Clear the terminal screen based on the operating system."""
    os.system('cls' if os.name == 'nt' else 'clear')


def fetch_data(symbol, timeframe, duration, end_date=None):
    """
    Fetch historical data for a symbol and timeframe, using caching to minimize API calls.

    STRATEGY CONTEXT:
    - Monthly data: Used to find yearly high/low price levels (support/resistance)
    - Weekly data: Used for long-term trend (200-week moving average)
    - Daily data: Used for medium-term trend and volume analysis

    Args:
        symbol: Stock symbol (e.g., 'AAPL' for Apple)
        timeframe: String ('monthly', 'weekly', 'daily')
        duration: Duration in days or months
        end_date: Optional end date for data request (datetime object)

    Returns:
        pandas DataFrame with historical data, or None if fetch fails
    """
    # Adjust intervals based on timeframe
    if timeframe == 'monthly':
        interval = '1mo'
    elif timeframe == 'weekly':
        interval = '1wk'
    else:  # daily
        interval = '1d'

    # Calculate start date based on duration
    if end_date is None:
        end_date = datetime.datetime.now()

    if timeframe == 'monthly':
        start_date = end_date - datetime.timedelta(days=30 * duration)
    elif timeframe == 'weekly':
        start_date = end_date - datetime.timedelta(days=7 * duration)
    else:  # daily
        start_date = end_date - datetime.timedelta(days=duration)

    cache_path = Path(f'cache/{symbol}_{timeframe}.h5')

    # Check if we have cached data from today
    print(cache_path.exists())
    print(datetime.date.fromtimestamp(cache_path.stat().st_mtime))
    print(datetime.date.today())
    print(datetime.date.fromtimestamp(cache_path.stat().st_mtime) == datetime.date.today())

    if cache_path.exists() and datetime.date.fromtimestamp(cache_path.stat().st_mtime) == datetime.date.today():
        return pd.read_hdf(cache_path)
    else:
        try:
            # Convert end_date to string format for yfinance
            end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
            start_date_str = start_date.strftime('%Y-%m-%d')

            # Download data from Yahoo Finance
            df = yf.download(
                symbol,
                start=start_date_str,
                end=end_date_str,
                interval=interval,
                progress=False
            )
            print(df.describe())
            print(df.head())
            print(df.tail())
            print(df.columns)
            print(df.index)
            print(df.dtypes)
            print(df.shape)
            print(df.info())

            if df is not None and not df.empty:
                # Save to cache
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_hdf(cache_path, key='df', mode='w')

                for col in df.columns:
                    print(col)

                # Rename columns to match original code's expectations
                df.columns = [col.lower() for col in df.columns]

                # Add date column if not present
                if 'date' not in df.columns:
                    df['date'] = df.index


            return df
        except Exception as e:
            print(f"Error fetching {timeframe} data for {symbol}: {e}")
            return None


def process_stock(symbol, last_end_date):
    """
    Process a single stock by fetching data and applying buy and sell conditions.

    STRATEGY IMPLEMENTATION:
    This function implements the core trend-following logic by:
    1. Finding last year's highest and lowest prices (key support/resistance levels)
    2. Calculating 200-period moving averages on weekly and daily timeframes
    3. Comparing current price to these levels to determine trend direction and strength

    Psychological basis: Prices breaking above last year's high often signal institutional buying
    and positive sentiment shift; prices below last year's low often indicate capitulation.

    Args:
        symbol: Stock symbol (e.g., 'AAPL')
        last_end_date: Datetime object for the end of last year
    """
    try:
        # Fetch monthly data for last year's highest high and lowest low
        # These levels represent significant psychological support/resistance
        monthly_df = fetch_data(symbol, 'monthly', 12, end_date=last_end_date)
        if monthly_df is None or monthly_df.empty:
            return

        # Fetch weekly data for 200-period SMA
        # The 200-week SMA represents the long-term trend direction
        weekly_df = fetch_data(symbol, 'weekly', 200)  # ~4 years of weekly data
        if weekly_df is None or len(weekly_df) < 200:
            return

        # Fetch daily data for 200-period SMA and closing price
        # The 200-day SMA is widely followed by institutions
        daily_df = fetch_data(symbol, 'daily', 365)  # 1 year of daily data
        if daily_df is None or len(daily_df) < 200:
            return

        # Calculate key technical levels
        last_year_highest_price = monthly_df["high"].max()
        last_year_lowest_price = monthly_df["low"].min()
        weekly_sma = weekly_df["close"].rolling(window=min(200, len(weekly_df))).mean().iloc[-1]
        daily_sma = daily_df["close"].rolling(window=min(200, len(daily_df))).mean().iloc[-1]
        closing_price = daily_df["close"].iloc[-1]

        # Check buy conditions:
        # 1. Price breaking above last year's highest high (major resistance breakout)
        # 2. Price above both 200-week and 200-day SMAs (confirming long-term uptrend)
        if (closing_price > last_year_highest_price and
                closing_price > weekly_sma and
                closing_price > daily_sma):
            buys.append(symbol)

        # Check sell conditions:
        # 1. Price breaking below last year's lowest low (major support breakdown)
        # 2. Price below both 200-week and 200-day SMAs (confirming long-term downtrend)
        if (closing_price < last_year_lowest_price and
                closing_price < weekly_sma and
                closing_price < daily_sma):
            sells.append(symbol)

    except Exception as e:
        print(f"Error processing {symbol}: {e}")


def add_volume_analysis(df, ma_period=10, volume_threshold=1.5, spark_threshold=2.0):
    """
    Adds Volume Price Analysis (VPA) indicators to identify smart money activity.

    STRATEGY CONTEXT:
    Volume analysis helps confirm price movements by showing institutional activity.
    - High volume on up days (accumulation): Institutions buying
    - High volume on down days (distribution): Institutions selling
    - Volume spikes (sparks): Potential turning points or exhaustion
    - Volume dips after spikes: Often consolidation before next move

    The combination of price action and volume provides insight into the conviction
    behind market moves and helps filter out false signals.

    Parameters:
    - df (pd.DataFrame): DataFrame with columns ['open', 'high', 'low', 'close', 'volume']
    - ma_period (int): Period for the Volume Moving Average (default is 10 days)
    - volume_threshold (float): Threshold for high volume relative to MA (default is 1.5)
    - spark_threshold (float): Threshold for spark day volume (default is 2.0)

    Returns:
    - pd.DataFrame: DataFrame with added volume analysis indicators
    """
    # Create a copy to avoid modifying the original
    vpa_df = df.copy()

    # Calculate volume moving average (baseline for normal volume)
    vpa_df['Volume_MA'] = vpa_df['volume'].rolling(window=ma_period).mean()

    # Calculate relative volume (how much higher/lower than normal)
    vpa_df['Volume_Ratio'] = vpa_df['volume'] / vpa_df['Volume_MA']

    # Calculate daily price change percentage
    vpa_df['Price_Change'] = vpa_df['close'].pct_change() * 100

    # Identify accumulation days (high volume + price increase)
    # These often indicate institutional buying
    vpa_df['Accumulation'] = (vpa_df['Volume_Ratio'] > volume_threshold) & (vpa_df['Price_Change'] > 0)

    # Identify distribution days (high volume + price decrease)
    # These often indicate institutional selling
    vpa_df['Distribution'] = (vpa_df['Volume_Ratio'] > volume_threshold) & (vpa_df['Price_Change'] < 0)

    # Identify volume spikes (very high relative volume)
    # These can signal climactic moves or turning points
    vpa_df['Spark'] = vpa_df['Volume_Ratio'] > spark_threshold

    # Identify low volume days (below average)
    # Often consolidation or lack of interest
    vpa_df['Dip'] = vpa_df['Volume_Ratio'] < 1.0

    return vpa_df


def detect_spark_and_dip(vpa_df, lookback=5):
    """
    Detects if there is a 'spark and dip' pattern in the recent data.

    STRATEGY CONTEXT:
    A 'spark and dip' pattern (high volume spike followed by low volume)
    often indicates a potential trend change after smart money has positioned.
    - Volume spike: Smart money making a large position
    - Volume dip: Consolidation before next move

    This pattern can help identify stocks where smart money has already accumulated
    and the stock is ready for its next move higher.

    Parameters:
    - vpa_df (pd.DataFrame): DataFrame with VPA indicators
    - lookback (int): Number of days to look back for the pattern

    Returns:
    - bool: True if the pattern is detected, False otherwise
    """
    if len(vpa_df) < 2:  # Need at least 2 days to detect a sequence
        return False

    # Make sure we don't try to access more rows than exist
    lookback = min(lookback, len(vpa_df) - 1)
    recent_df = vpa_df.iloc[-lookback:]

    # Look for a volume spike followed by a volume dip
    for i in range(len(recent_df) - 1):
        if recent_df['Spark'].iloc[i] and recent_df['Dip'].iloc[i + 1]:
            return True
    return False


def get_us_stock_list():
    """
    Returns a list of US stocks to scan.

    This focuses on:
    1. S&P 500 components (large cap, highly liquid)
    2. Some popular mid-cap stocks
    3. Key sector ETFs for overall market analysis

    Returns:
        List of stock symbols
    """
    # S&P 500 tracking ETF for overall market
    major_etfs = [
        'SPY',  # S&P 500
        'QQQ',  # Nasdaq 100
        'DIA',  # Dow Jones Industrial Average
        'IWM',  # Russell 2000 (small caps)
    ]

    # Sector ETFs for sector analysis
    sector_etfs = [
        'XLF',  # Financials
        'XLK',  # Technology
        'XLE',  # Energy
        'XLV',  # Healthcare
        'XLI',  # Industrials
        'XLP',  # Consumer Staples
        'XLY',  # Consumer Discretionary
        'XLB',  # Materials
        'XLU',  # Utilities
        'XLRE',  # Real Estate
    ]

    # Major stocks by sector - tech heavy due to market influence
    tech_stocks = [
        'AAPL',  # Apple
        'MSFT',  # Microsoft
        'GOOGL',  # Alphabet (Google)
        'AMZN',  # Amazon
        'META',  # Meta (Facebook)
        'NVDA',  # NVIDIA
        'TSLA',  # Tesla
        'AVGO',  # Broadcom
        'ORCL',  # Oracle
        'CRM',  # Salesforce
        'AMD',  # Advanced Micro Devices
        'INTC',  # Intel
    ]

    financial_stocks = [
        'JPM',  # JPMorgan Chase
        'BAC',  # Bank of America
        'WFC',  # Wells Fargo
        'GS',  # Goldman Sachs
        'MS',  # Morgan Stanley
    ]

    healthcare_stocks = [
        'JNJ',  # Johnson & Johnson
        'PFE',  # Pfizer
        'ABBV',  # AbbVie
        'MRK',  # Merck
        'LLY',  # Eli Lilly
    ]

    consumer_stocks = [
        'PG',  # Procter & Gamble
        'KO',  # Coca-Cola
        'PEP',  # PepsiCo
        'WMT',  # Walmart
        'COST',  # Costco
        'MCD',  # McDonald's
        'NKE',  # Nike
    ]

    industrial_stocks = [
        'CAT',  # Caterpillar
        'BA',  # Boeing
        'GE',  # General Electric
        'HON',  # Honeywell
        'UPS',  # UPS
    ]

    energy_stocks = [
        'XOM',  # Exxon Mobil
        'CVX',  # Chevron
        'COP',  # ConocoPhillips
    ]

    # Combine all lists
    # all_stocks = (major_etfs + sector_etfs + tech_stocks + financial_stocks +
    #              healthcare_stocks + consumer_stocks + industrial_stocks + energy_stocks)
    all_stocks = (['SPY', 'CSCO'])

    # Return unique symbols (in case of duplicates)
    return list(set(all_stocks))


def main():
    """
    Main function to run the stock scanner with US market stocks.

    This function:
    1. Gets a list of US stocks to scan
    2. Processes each stock against the strategy criteria
    3. Performs additional volume analysis on qualified stocks
    4. Saves results to a CSV file
    """
    clear_screen()
    print("Starting Yahoo Finance US Stock Scanner...")
    print("Strategy: Trend Following with Volume Confirmation")
    print("-" * 50)

    # Set last year's end date for yearly high/low calculation
    current_date = datetime.date.today()
    last_year = current_date.year - 1
    last_end_date = datetime.datetime(last_year, 12, 31, 23, 59, 59)

    # Get list of US stocks to scan
    symbols = get_us_stock_list()
    print(f"Loaded {len(symbols)} US stocks and ETFs to scan")

    # Process each stock
    clear_screen()
    print(f"Processing {len(symbols)} stocks...")
    print("Finding stocks that are:")
    print("  - Breaking above last year's high (or below last year's low)")
    print("  - Above/below 200-period moving averages on both weekly and daily timeframes")
    print("-" * 50)

    for i, symbol in enumerate(symbols):
        print(f"Processing {i + 1}/{len(symbols)}: {symbol}")
        process_stock(symbol, last_end_date)
        # Add a short delay to avoid hitting rate limits
        if (i + 1) % 10 == 0:
            time.sleep(1)  # Pause briefly every 10 stocks

    print("\nPrimary analysis complete.")
    print(f"Stocks with bullish trend signals: {len(buys)}")
    print(buys)
    print(f"\nStocks with bearish trend signals: {len(sells)}")
    print(sells)

    # Volume analysis on buys to filter for highest quality opportunities
    print("\nPerforming volume analysis on bullish candidates...")
    print("Looking for accumulation patterns and spark-dip volume sequences...")
    promising_buys = []
    for symbol in buys:
        daily_df = fetch_data(symbol, 'daily', 365)
        if daily_df is not None:
            vpa_df = add_volume_analysis(daily_df)
            if not vpa_df.empty:
                last_row = vpa_df.iloc[-1]
                # Check for accumulation and spark-dip pattern
                # This confirms institutional buying with a consolidation phase
                if 'Accumulation' in last_row and last_row['Accumulation'] and detect_spark_and_dip(vpa_df, lookback=5):
                    promising_buys.append(symbol)

    print("\nHighest probability buy candidates (strong trend + volume confirmation):")
    print(promising_buys)

    # Volume analysis on sells
    print("\nPerforming volume analysis on bearish candidates...")
    print("Looking for distribution patterns (heavy selling on down days)...")
    promising_sells = []
    for symbol in sells:
        daily_df = fetch_data(symbol, 'daily', 365)
        if daily_df is not None:
            vpa_df = add_volume_analysis(daily_df)
            if not vpa_df.empty:
                last_row = vpa_df.iloc[-1]
                # Check for distribution (institutional selling)
                if 'Distribution' in last_row and last_row['Distribution']:
                    promising_sells.append(symbol)

    print("\nHighest probability sell candidates (weak trend + volume confirmation):")
    print(promising_sells)

    # Write results to a file
    results_dict = {
        "RawBuys": buys,
        "RawSells": sells,
        "PromisingBuys": promising_buys,
        "PromisingSells": promising_sells
    }

    # Convert to DataFrame with equal length columns
    max_len = max(len(v) for v in results_dict.values())
    for k, v in results_dict.items():
        v.extend([np.nan] * (max_len - len(v)))

    result_frame = pd.DataFrame(results_dict)
    result_filename = f"us_market_scan_{datetime.date.today().strftime('%Y%m%d')}.csv"
    result_frame.to_csv(result_filename, index=False)
    print(f"\nResults written to {result_filename}")

    print("\nSCAN SUMMARY:")
    print(f"Total bullish candidates: {len(buys)}")
    print(f"High probability buys: {len(promising_buys)}")
    print(f"Total bearish candidates: {len(sells)}")
    print(f"High probability sells: {len(promising_sells)}")
    print("\nStrategy reminder:")
    print("- Bullish signals: Price > last year's high + above 200-period SMAs + volume confirmation")
    print("- Bearish signals: Price < last year's low + below 200-period SMAs + volume confirmation")


if __name__ == "__main__":
    Path('cache').mkdir(exist_ok=True)
    main()