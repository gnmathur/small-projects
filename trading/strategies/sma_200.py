import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf
from datetime import datetime, timedelta
import pandas_market_calendars as mcal

def get_spy_data(start_date='2000-01-01', end_date=None):
    """
    Download SPY historical data from Yahoo Finance
    """
    if end_date is None:
        end_date = datetime.today().strftime('%Y-%m-%d')

    # Download SPY data with explicit auto_adjust parameter
    spy = yf.download('SPY', start=start_date, end=end_date, auto_adjust=False)
    return spy

def calculate_200dma(df):
    """
    Calculate 200-day moving average
    """
    # Use 'Close' if 'Adj Close' is not available
    price_column = 'Adj Close' if 'Adj Close' in df.columns else 'Close'
    df['200DMA'] = df[price_column].rolling(window=200).mean()
    return df, price_column

def get_month_end_dates(start_date, end_date):
    """
    Get list of month-end dates that are trading days
    """
    # Get NYSE calendar
    nyse = mcal.get_calendar('NYSE')

    # Get all trading days
    trading_days = nyse.schedule(start_date=start_date, end_date=end_date)
    trading_days_idx = pd.DatetimeIndex(trading_days.index)

    # Convert to DataFrame for easier manipulation
    trading_days_df = pd.DataFrame(index=trading_days_idx)
    trading_days_df['year'] = trading_days_df.index.year
    trading_days_df['month'] = trading_days_df.index.month

    # Group by year and month to get month-end dates
    grouped = trading_days_df.groupby(['year', 'month'])
    month_ends = []

    for name, group in grouped:
        # Get the last trading day of the month
        month_ends.append(group.index[-1])

    return month_ends

def implement_strategy(df, month_end_dates, price_column):
    """
    Implement the 200 DMA strategy at month-end dates

    Strategy:
    - Buy SPY when price rises above 200 DMA at month end
    - Sell SPY when price falls below 200 DMA at month end
    """
    # Initialize strategy variables
    position = 0  # 0: no position, 1: long
    trades = []
    portfolio_values = []
    initial_capital = 10000
    cash = initial_capital
    shares = 0

    # Create signals DataFrame
    signals = pd.DataFrame(index=df.index)
    signals['price'] = df[price_column]
    signals['200dma'] = df['200DMA']
    signals['position'] = 0
    signals['signal'] = 0

    # Loop through each month-end date
    for date in month_end_dates:
        # Convert to Timestamp for proper indexing if it's not already
        date = pd.Timestamp(date)

        # Skip if date is not in our data
        if date not in df.index:
            continue

        try:
            # Get the 200DMA value safely
            dma_value = df.loc[date, '200DMA']
            # Handle case where multiple rows match this date
            if isinstance(dma_value, pd.Series):
                dma_value = dma_value.iloc[0]
            # Skip if we don't have 200DMA value yet
            if pd.isna(dma_value):
                continue

            # Get price safely
            price = df.loc[date, price_column]
            if isinstance(price, pd.Series):
                price = price.iloc[0]

        except Exception as e:
            # Skip on any error
            print(f"Error processing date {date}: {e}")
            continue

        # Check if price is above or below 200 DMA
        if price > dma_value and position == 0:
            # Buy signal
            signals.loc[date, 'signal'] = 1
            position = 1
            shares = cash / price
            trade_value = shares * price
            cash = cash - trade_value
            trades.append({
                'date': date,
                'action': 'BUY',
                'price': price,
                'shares': shares,
                'value': trade_value,
                'cash': cash
            })

        elif price < dma_value and position == 1:
            # Sell signal
            signals.loc[date, 'signal'] = -1
            position = 0
            trade_value = shares * price
            cash = cash + trade_value
            trades.append({
                'date': date,
                'action': 'SELL',
                'price': price,
                'shares': shares,
                'value': trade_value,
                'cash': cash
            })
            shares = 0

        # Update position column
        signals.loc[date:, 'position'] = position

        # Calculate portfolio value
        portfolio_value = cash + (shares * price)
        portfolio_values.append({
            'date': date,
            'portfolio_value': portfolio_value,
            'cash': cash,
            'stock_value': shares * price,
            'price': price,
            'position': position
        })

    # Convert trades and portfolio values to DataFrame
    trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()
    portfolio_df = pd.DataFrame(portfolio_values) if portfolio_values else pd.DataFrame()

    # Forward fill the position using ffill() instead of fillna(method='ffill')
    signals['position'] = signals['position'].ffill()

    return signals, trades_df, portfolio_df

def calculate_performance(portfolio_df, signals_df, spy_df, price_column):
    """
    Calculate strategy performance metrics
    """
    if portfolio_df.empty:
        return {
            'total_return': 0,
            'annualized_return': 0,
            'sharpe_ratio': 0,
            'max_drawdown': 0,
            'trades_count': 0,
            'buy_hold_return': 0
        }

    # Calculate returns
    portfolio_df['daily_return'] = portfolio_df['portfolio_value'].pct_change()

    # Get buy-and-hold returns for comparison
    start_date = portfolio_df['date'].min()
    end_date = portfolio_df['date'].max()

    # Find closest index dates
    start_idx = spy_df.index.get_indexer([start_date], method='nearest')[0]
    end_idx = spy_df.index.get_indexer([end_date], method='nearest')[0]

    # Get the price values making sure they're scalar
    spy_start_price = spy_df.iloc[start_idx][price_column]
    if isinstance(spy_start_price, pd.Series):
        spy_start_price = spy_start_price.iloc[0]

    spy_end_price = spy_df.iloc[end_idx][price_column]
    if isinstance(spy_end_price, pd.Series):
        spy_end_price = spy_end_price.iloc[0]

    buy_hold_return = (spy_end_price / spy_start_price) - 1

    # Calculate strategy returns
    start_value = portfolio_df['portfolio_value'].iloc[0]
    end_value = portfolio_df['portfolio_value'].iloc[-1]
    strategy_return = (end_value / start_value) - 1

    # Calculate annualized return
    years = (pd.to_datetime(portfolio_df['date'].max()) -
             pd.to_datetime(portfolio_df['date'].min())).days / 365.25

    if years > 0:
        annualized_return = (1 + strategy_return) ** (1 / years) - 1
    else:
        annualized_return = 0

    # Calculate Sharpe Ratio (using risk-free rate of 0 for simplicity)
    if not signals_df.empty:
        daily_returns = signals_df['price'].pct_change() * signals_df['position'].shift(1)
        if len(daily_returns) > 1 and daily_returns.std() > 0:
            sharpe_ratio = np.sqrt(252) * daily_returns.mean() / daily_returns.std()
        else:
            sharpe_ratio = 0
    else:
        sharpe_ratio = 0

    # Calculate Maximum Drawdown
    if not portfolio_df.empty:
        portfolio_df['cum_max'] = portfolio_df['portfolio_value'].cummax()
        portfolio_df['drawdown'] = (portfolio_df['portfolio_value'] / portfolio_df['cum_max']) - 1
        max_drawdown = portfolio_df['drawdown'].min()
    else:
        max_drawdown = 0

    # Count the number of trades
    trades_count = len(portfolio_df[portfolio_df['position'] != portfolio_df['position'].shift(1)]) - 1
    if trades_count < 0:
        trades_count = 0

    return {
        'total_return': strategy_return * 100,
        'buy_hold_return': buy_hold_return * 100,
        'annualized_return': annualized_return * 100,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': max_drawdown * 100,
        'trades_count': trades_count
    }

def plot_results(spy_df, signals_df, portfolio_df, price_column):
    """
    Plot the results of the strategy
    """
    if portfolio_df.empty:
        print("No trades were executed. Not enough data or no signals generated.")
        return

    plt.figure(figsize=(15, 12))

    # Plot 1: SPY Price and 200 DMA with Buy/Sell signals
    plt.subplot(3, 1, 1)
    plt.plot(spy_df.index, spy_df[price_column], label='SPY Price')
    plt.plot(spy_df.index, spy_df['200DMA'], label='200 DMA', alpha=0.6)

    # Plot buy signals
    buy_signals = signals_df[signals_df['signal'] == 1]
    if not buy_signals.empty:
        plt.scatter(buy_signals.index, buy_signals['price'], marker='^', color='green', s=100, label='Buy Signal')

    # Plot sell signals
    sell_signals = signals_df[signals_df['signal'] == -1]
    if not sell_signals.empty:
        plt.scatter(sell_signals.index, sell_signals['price'], marker='v', color='red', s=100, label='Sell Signal')

    plt.title('SPY Price with 200 DMA and Trading Signals')
    plt.ylabel('Price ($)')
    plt.grid(True)
    plt.legend()

    # Plot 2: Portfolio Value over time
    plt.subplot(3, 1, 2)
    plt.plot(portfolio_df['date'], portfolio_df['portfolio_value'], label='Portfolio Value')
    plt.title('Portfolio Value Over Time')
    plt.ylabel('Value ($)')
    plt.grid(True)
    plt.legend()

    # Plot 3: Strategy position
    plt.subplot(3, 1, 3)
    plt.plot(signals_df.index, signals_df['position'], label='Position (1=Long, 0=Cash)')
    plt.title('Strategy Position')
    plt.ylabel('Position')
    plt.ylim(-0.1, 1.1)
    plt.grid(True)
    plt.legend()

    plt.tight_layout()
    plt.show()

def main():
    """
    Main function to run the strategy
    """
    # Define date range
    start_date = '2020-01-01'
    end_date = datetime.today().strftime('%Y-%m-%d')

    # Get SPY data
    print("Downloading SPY data...")
    spy_df = get_spy_data(start_date, end_date)

    # Calculate 200 DMA
    print("Calculating 200-day moving average...")
    spy_df, price_column = calculate_200dma(spy_df)

    # Get month-end dates
    print("Determining month-end trading days...")
    month_end_dates = get_month_end_dates(start_date, end_date)

    # Implement strategy
    print("Implementing trading strategy...")
    signals, trades, portfolio = implement_strategy(spy_df, month_end_dates, price_column)

    # Calculate performance
    print("Calculating performance metrics...")
    performance = calculate_performance(portfolio, signals, spy_df, price_column)

    # Display results
    print("\nStrategy Performance:")
    print(f"Total Return: {performance['total_return']:.2f}%")
    print(f"Buy and Hold Return: {performance['buy_hold_return']:.2f}%")
    print(f"Annualized Return: {performance['annualized_return']:.2f}%")
    print(f"Sharpe Ratio: {performance['sharpe_ratio']:.2f}")
    print(f"Maximum Drawdown: {performance['max_drawdown']:.2f}%")
    print(f"Number of Trades: {performance['trades_count']}")

    # Display recent trades
    if not trades.empty:
        print("\nMost Recent Trades:")
        print(trades.tail(5).to_string(index=False))
    else:
        print("\nNo trades were executed.")

    # Plot results
    print("\nGenerating plots...")
    plot_results(spy_df, signals, portfolio, price_column)


if __name__ == '__main__':
    main()