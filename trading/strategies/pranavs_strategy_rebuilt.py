# -*- coding: utf-8 -*-
import datetime
import os

from providers.yahoo import get_yahoo_finance_data, YahooFinanceCache
import matplotlib.pyplot as plt

def clear_screen():
    """Clear the terminal screen based on the operating system."""
    os.system('cls' if os.name == 'nt' else 'clear')

def plot_weekly_data(weekly_df):
    plt.figure(figsize=(14, 7))
    plt.plot(weekly_df['close'], label='Close Price', color='blue')
    plt.plot(weekly_df['sma_200wk'], label='200-Day SMA', color='orange', linewidth=2)

    plt.title('Closing Price & 200-Day SMA')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.show()


def ph(lbl, msg: object):
    """Print the header with asterisks."""
    print("-" * 10 + f" {lbl} " + "-" * 10)
    print(msg)

cache = YahooFinanceCache("cache_parquet", 1)

buys = []  # List to hold stocks to buy
sells = []  # List to hold stocks to sell

def process_stock(ticker):
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
        last_year_start = datetime.date.today().replace(year=datetime.date.today().year - 1, month=1, day=1)
        last_year_start_ymd = last_year_start.strftime('%Y-%m-%d')
        last_year_end = datetime.date.today().replace(year=datetime.date.today().year - 1, month=12, day=31)
        last_year_end_ymd = last_year_end.strftime('%Y-%m-%d')

        monthly_df = get_yahoo_finance_data(cache, [ticker], '1mo', start=last_year_start_ymd, end=last_year_end_ymd)
        if monthly_df is None or monthly_df.empty:
            return

        # Fetch weekly data for 200-period SMA
        # The 200-week SMA represents the long-term trend direction
        weekly_df = get_yahoo_finance_data(cache, [ticker], '1wk', period='200wk')  # ~4 years of weekly data
        weekly_df['close'].to_csv("weekly_close.csv")

        if weekly_df is None or len(weekly_df) < 200:
            return

        # Fetch daily data for 200-period SMA and closing price
        # The 200-day SMA is widely followed by institutions
        daily_df = get_yahoo_finance_data(cache, [ticker], '1d', period='365d')  # 1 year of daily data
        if daily_df is None or len(daily_df) != 365:
            return

        # Calculate key technical levels
        last_year_highest_price = monthly_df["high"].max()
        last_year_lowest_price = monthly_df["low"].min()

        weekly_sma = weekly_df["close"].rolling(window=min(200, len(weekly_df))).mean().iloc[-1]
        daily_sma = daily_df["close"].rolling(window=min(200, len(daily_df))).mean().iloc[-1]

        closing_price = daily_df["close"].iloc[-1]

        ph("1", f"Ticker: {ticker}")
        ph("2", f"Last Year High: {last_year_highest_price}")
        ph("3", f"Last Year Low: {last_year_lowest_price}")
        ph("4", f"Weekly 200 SMA: {weekly_sma}")
        ph("5", f"Daily 200 SMA: {daily_sma}")
        ph("6", f"Current Price: {closing_price}")
        ph("7", f"Weekly Data Length: {len(weekly_df)}")

        # Check buy conditions:
        # 1. Price breaking above last year's highest high (major resistance breakout)
        # 2. Price above both 200-week and 200-day SMAs (confirming long-term uptrend)
        if (closing_price > last_year_highest_price and
                closing_price > weekly_sma and
                closing_price > daily_sma):
            buys.append(ticker)

        # Check sell conditions:
        # 1. Price breaking below last year's lowest low (major support breakdown)
        # 2. Price below both 200-week and 200-day SMAs (confirming long-term downtrend)
        if (closing_price < last_year_lowest_price and
                closing_price < weekly_sma and
                closing_price < daily_sma):
            sells.append(ticker)

    except Exception as e:
        print(f"Error processing {ticker}: {e}")

def main():
    process_stock("AAPL")
    print(buys)
    print(sells)

if __name__ == "__main__":
    main()