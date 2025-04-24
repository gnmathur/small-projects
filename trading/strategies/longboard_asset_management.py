import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import logging
import time
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trend_following.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TrendFollowingStrategy:
    def __init__(self,
                 initial_capital=1000000,
                 target_volatility=0.15,
                 max_leverage=2.0,
                 min_price=10,
                 min_adv_usd=1000000,
                 atr_period=42,
                 atr_multiplier=10,
                 min_weight_change=0.005,
                 data_dir="data",
                 universe_file="russell3000.csv"):
        """
        Initialize the trend following strategy with the parameters specified in the paper.

        Parameters:
        -----------
        initial_capital : float
            Initial portfolio value
        target_volatility : float
            Target portfolio volatility (annualized)
        max_leverage : float
            Maximum portfolio leverage
        min_price : float
            Minimum stock price filter
        min_adv_usd : float
            Minimum 42-day average daily dollar volume (inflation-adjusted)
        atr_period : int
            Period for calculating ATR (Average True Range)
        atr_multiplier : float
            Multiplier for the ATR-based trailing stop
        min_weight_change : float
            Minimum weight change required to trigger a rebalance
        data_dir : str
            Directory to store data
        universe_file : str
            File containing the ticker universe (Russell 3000)
        """
        self.initial_capital = initial_capital
        self.portfolio_value = initial_capital
        self.cash = initial_capital
        self.target_volatility = target_volatility
        self.max_leverage = max_leverage
        self.min_price = min_price
        self.min_adv_usd = min_adv_usd
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier
        self.min_weight_change = min_weight_change
        self.data_dir = data_dir
        self.universe_file = universe_file

        # Create data directory if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        # Portfolio tracking
        self.positions = {}  # Current positions {ticker: {'shares': n, 'entry_price': p, 'stop_price': s, 'ATH': h}}
        self.portfolio_history = []  # Track portfolio value over time
        self.trades = []  # Track all trades

        # Load universe
        self.load_universe()

    def load_universe(self):
        """Load the Russell 3000 universe or create a placeholder if file doesn't exist."""
        try:
            self.universe = pd.read_csv(os.path.join(self.data_dir, self.universe_file))
            logger.info(f"Loaded {len(self.universe)} stocks from universe file")
        except FileNotFoundError:
            logger.warning(f"Universe file not found. Creating placeholder.")
            # As a fallback, we can use the current S&P 500 components
            # In a real implementation, you would want to get the actual Russell 3000
            sp500 = pd.DataFrame({'ticker': ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'JNJ']})
            sp500.to_csv(os.path.join(self.data_dir, self.universe_file), index=False)
            self.universe = sp500
            logger.info(f"Created placeholder universe with {len(self.universe)} stocks")

    def get_historical_data(self, tickers, period="2y"):
        """
        Fetch historical data for a list of tickers.

        Parameters:
        -----------
        tickers : list
            List of ticker symbols
        period : str
            Lookback period for data

        Returns:
        --------
        dict
            Dictionary of DataFrames with historical data for each ticker
        """
        data = {}
        for ticker in tickers:
            try:
                # Download data with retry mechanism
                for attempt in range(3):
                    try:
                        stock_data = yf.download(ticker, period=period, progress=False)
                        if len(stock_data) > 0:
                            data[ticker] = stock_data
                            break
                    except Exception as e:
                        if attempt == 2:
                            logger.error(f"Failed to download data for {ticker} after 3 attempts: {e}")
                        else:
                            time.sleep(1)  # Wait before retrying
            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")

        return data

    def calculate_atr(self, data):
        """
        Calculate the Average True Range (ATR) for a given DataFrame.

        Parameters:
        -----------
        data : pd.DataFrame
            DataFrame with OHLC data

        Returns:
        --------
        pd.DataFrame
            DataFrame with ATR column added
        """
        high = data['High']
        low = data['Low']
        close = data['Close'].shift(1)

        tr1 = high - low
        tr2 = abs(high - close)
        tr3 = abs(low - close)

        tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        atr = tr.rolling(window=self.atr_period).mean()

        data['ATR'] = atr
        data['ATR_pct'] = data['ATR'] / data['Close']

        # Calculate annualized volatility
        data['Volatility'] = data['ATR_pct'] * np.sqrt(252)

        return data

    def calculate_all_time_high(self, data):
        """
        Calculate the all-time high for a given DataFrame.

        Parameters:
        -----------
        data : pd.DataFrame
            DataFrame with OHLC data

        Returns:
        --------
        pd.DataFrame
            DataFrame with ATH column added
        """
        data['ATH'] = data['Close'].cummax()
        data['ATH_breakout'] = (data['Close'] == data['ATH']) & (data['Close'] > data['Close'].shift(1))

        return data

    def calculate_stop_price(self, data):
        """
        Calculate the ATR-based trailing stop price.

        Parameters:
        -----------
        data : pd.DataFrame
            DataFrame with OHLC and ATR data

        Returns:
        --------
        pd.DataFrame
            DataFrame with Stop_Price column added
        """
        # Stop_t = ATH_t × (1 − (ATR_t / Close_t))^10
        data['Stop_Price'] = data['ATH'] * (1 - (data['ATR'] / data['Close'])) ** self.atr_multiplier

        return data

    def calculate_position_size(self, data, current_date):
        """
        Calculate position sizes based on volatility targeting.

        Parameters:
        -----------
        data : dict
            Dictionary of DataFrames with historical data for each ticker
        current_date : datetime
            Current simulation date

        Returns:
        --------
        dict
            Dictionary with target weights for each ticker
        """
        weights = {}
        valid_tickers = []

        # First pass: filter universe based on criteria
        for ticker, df in data.items():
            if len(df) < self.atr_period + 10:
                continue

            latest = df.iloc[-1]

            # Apply filters
            if latest['Close'] < self.min_price:
                continue

            # Calculate average daily dollar volume
            df['Dollar_Volume'] = df['Close'] * df['Volume']
            adv = df['Dollar_Volume'].rolling(window=self.atr_period).mean()
            if adv.iloc[-1] < self.min_adv_usd:
                continue

            valid_tickers.append(ticker)

        # Second pass: identify breakouts and calculate weights
        breakout_tickers = []
        for ticker in valid_tickers:
            df = data[ticker]
            if df.iloc[-1]['ATH_breakout']:
                breakout_tickers.append(ticker)

        if not breakout_tickers:
            return weights

        # Calculate weights based on volatility
        total_inverse_vol = 0
        ticker_inverse_vols = {}

        for ticker in breakout_tickers:
            vol = data[ticker].iloc[-1]['Volatility']
            if vol > 0:
                inverse_vol = 1.0 / vol
                ticker_inverse_vols[ticker] = inverse_vol
                total_inverse_vol += inverse_vol

        # Normalize weights
        if total_inverse_vol > 0:
            for ticker, inverse_vol in ticker_inverse_vols.items():
                weights[ticker] = (inverse_vol / total_inverse_vol) * (
                            self.target_volatility / data[ticker].iloc[-1]['Volatility'])

            # Apply leverage cap
            total_weight = sum(abs(w) for w in weights.values())
            if total_weight > self.max_leverage:
                scaling_factor = self.max_leverage / total_weight
                weights = {t: w * scaling_factor for t, w in weights.items()}

        return weights

    def update_stops(self, data, current_date):
        """
        Update trailing stops for existing positions.

        Parameters:
        -----------
        data : dict
            Dictionary of DataFrames with historical data for each ticker
        current_date : datetime
            Current simulation date
        """
        for ticker in list(self.positions.keys()):
            if ticker in data:
                position = self.positions[ticker]
                latest = data[ticker].iloc[-1]

                # Update ATH if needed
                if latest['Close'] > position['ATH']:
                    position['ATH'] = latest['Close']

                # Calculate new stop price
                new_stop = position['ATH'] * (1 - (latest['ATR'] / latest['Close'])) ** self.atr_multiplier

                # Only raise stops, never lower them
                if new_stop > position['stop_price']:
                    position['stop_price'] = new_stop
                    logger.info(f"{current_date}: Raised stop for {ticker} to {new_stop:.2f}")

    def check_exits(self, data, current_date):
        """
        Check for exit signals based on trailing stops.

        Parameters:
        -----------
        data : dict
            Dictionary of DataFrames with historical data for each ticker
        current_date : datetime
            Current simulation date

        Returns:
        --------
        list
            List of tickers to exit
        """
        exits = []

        for ticker in list(self.positions.keys()):
            if ticker in data:
                position = self.positions[ticker]
                latest = data[ticker].iloc[-1]

                # Exit if close price is below stop price
                if latest['Close'] < position['stop_price']:
                    exits.append(ticker)
                    logger.info(
                        f"{current_date}: Exit signal for {ticker} - Close: {latest['Close']:.2f}, Stop: {position['stop_price']:.2f}")

        return exits

    def execute_trades(self, target_weights, data, current_date):
        """
        Execute trades based on target weights and exit signals.

        Parameters:
        -----------
        target_weights : dict
            Dictionary with target weights for each ticker
        data : dict
            Dictionary of DataFrames with historical data for each ticker
        current_date : datetime
            Current simulation date
        """
        # First handle exits
        exits = self.check_exits(data, current_date)
        for ticker in exits:
            self.exit_position(ticker, data[ticker].iloc[-1]['Close'], current_date)

        # Calculate current weights
        current_weights = {}
        for ticker, position in self.positions.items():
            if ticker in data:
                current_price = data[ticker].iloc[-1]['Close']
                position_value = position['shares'] * current_price
                current_weights[ticker] = position_value / self.portfolio_value

        # Determine required trades
        trades_to_execute = {}

        # Exit positions not in target weights
        for ticker in list(self.positions.keys()):
            if ticker not in target_weights:
                trades_to_execute[ticker] = 0

        # Enter or adjust positions based on target weights
        for ticker, target_weight in target_weights.items():
            current_weight = current_weights.get(ticker, 0)
            weight_diff = target_weight - current_weight

            # Only trade if weight change exceeds threshold
            if abs(weight_diff) >= self.min_weight_change:
                trades_to_execute[ticker] = target_weight

        # Execute the trades
        for ticker, target_weight in trades_to_execute.items():
            if ticker in self.positions and target_weight == 0:
                self.exit_position(ticker, data[ticker].iloc[-1]['Close'], current_date)
            elif ticker in self.positions:
                self.adjust_position(ticker, target_weight, data[ticker], current_date)
            elif target_weight > 0:
                self.enter_position(ticker, target_weight, data[ticker], current_date)

    def enter_position(self, ticker, weight, data, current_date):
        """
        Enter a new position.

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        weight : float
            Target portfolio weight
        data : pd.DataFrame
            DataFrame with OHLC and ATR data for the ticker
        current_date : datetime
            Current simulation date
        """
        latest = data.iloc[-1]
        price = latest['Close']

        # Calculate shares to buy
        target_value = self.portfolio_value * weight
        shares = int(target_value / price)

        if shares > 0:
            cost = shares * price

            # Check if we have enough cash
            if cost <= self.cash:
                self.cash -= cost

                # Record the position
                self.positions[ticker] = {
                    'shares': shares,
                    'entry_price': price,
                    'stop_price': latest['Stop_Price'],
                    'ATH': latest['Close'],
                    'entry_date': current_date
                }

                # Record the trade
                self.trades.append({
                    'date': current_date,
                    'ticker': ticker,
                    'action': 'BUY',
                    'shares': shares,
                    'price': price,
                    'cost': cost,
                    'stop_price': latest['Stop_Price']
                })

                logger.info(
                    f"{current_date}: Entered {ticker} - Shares: {shares}, Price: {price:.2f}, Weight: {weight:.2f}, Stop: {latest['Stop_Price']:.2f}")
            else:
                logger.warning(f"{current_date}: Not enough cash to enter {ticker} position")

    def adjust_position(self, ticker, new_weight, data, current_date):
        """
        Adjust an existing position.

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        new_weight : float
            New target portfolio weight
        data : pd.DataFrame
            DataFrame with OHLC and ATR data for the ticker
        current_date : datetime
            Current simulation date
        """
        current_position = self.positions[ticker]
        price = data.iloc[-1]['Close']

        # Calculate current position value and weight
        current_value = current_position['shares'] * price
        current_weight = current_value / self.portfolio_value

        # Calculate shares to buy or sell
        target_value = self.portfolio_value * new_weight
        target_shares = int(target_value / price)
        shares_diff = target_shares - current_position['shares']

        if shares_diff > 0:
            # Buying more shares
            cost = shares_diff * price
            if cost <= self.cash:
                self.cash -= cost
                current_position['shares'] += shares_diff

                # Record the trade
                self.trades.append({
                    'date': current_date,
                    'ticker': ticker,
                    'action': 'INCREASE',
                    'shares': shares_diff,
                    'price': price,
                    'cost': cost,
                    'stop_price': current_position['stop_price']
                })

                logger.info(
                    f"{current_date}: Increased {ticker} - Added: {shares_diff}, New Shares: {current_position['shares']}, Price: {price:.2f}, New Weight: {new_weight:.2f}")
            else:
                logger.warning(f"{current_date}: Not enough cash to increase {ticker} position")

        elif shares_diff < 0:
            # Selling some shares
            proceeds = abs(shares_diff) * price
            self.cash += proceeds
            current_position['shares'] += shares_diff  # will be negative

            # Record the trade
            self.trades.append({
                'date': current_date,
                'ticker': ticker,
                'action': 'DECREASE',
                'shares': abs(shares_diff),
                'price': price,
                'proceeds': proceeds,
                'stop_price': current_position['stop_price']
            })

            logger.info(
                f"{current_date}: Decreased {ticker} - Removed: {abs(shares_diff)}, New Shares: {current_position['shares']}, Price: {price:.2f}, New Weight: {new_weight:.2f}")

    def exit_position(self, ticker, price, current_date):
        """
        Exit an existing position.

        Parameters:
        -----------
        ticker : str
            Ticker symbol
        price : float
            Exit price
        current_date : datetime
            Current simulation date
        """
        position = self.positions[ticker]
        proceeds = position['shares'] * price
        self.cash += proceeds

        # Calculate P&L
        entry_value = position['shares'] * position['entry_price']
        pnl = proceeds - entry_value
        pnl_pct = (price / position['entry_price'] - 1) * 100

        # Calculate holding period
        holding_days = (current_date - position['entry_date']).days

        # Record the trade
        self.trades.append({
            'date': current_date,
            'ticker': ticker,
            'action': 'SELL',
            'shares': position['shares'],
            'price': price,
            'proceeds': proceeds,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'holding_days': holding_days
        })

        logger.info(
            f"{current_date}: Exited {ticker} - Shares: {position['shares']}, Price: {price:.2f}, P&L: ${pnl:.2f} ({pnl_pct:.2f}%), Holding Period: {holding_days} days")

        # Remove the position
        del self.positions[ticker]

    def update_portfolio_value(self, data, current_date):
        """
        Update the portfolio value.

        Parameters:
        -----------
        data : dict
            Dictionary of DataFrames with historical data for each ticker
        current_date : datetime
            Current simulation date
        """
        position_value = 0

        for ticker, position in self.positions.items():
            if ticker in data:
                price = data[ticker].iloc[-1]['Close']
                position_value += position['shares'] * price

        self.portfolio_value = self.cash + position_value

        # Record portfolio history
        self.portfolio_history.append({
            'date': current_date,
            'portfolio_value': self.portfolio_value,
            'cash': self.cash,
            'position_value': position_value,
            'num_positions': len(self.positions)
        })

        # Log portfolio value
        if len(self.portfolio_history) % 20 == 0:  # Log every 20 days to avoid too much output
            logger.info(
                f"{current_date}: Portfolio Value: ${self.portfolio_value:.2f}, Cash: ${self.cash:.2f}, Positions: {len(self.positions)}")

    def run_backtest(self, start_date=None, end_date=None, rebalance_freq='W-MON'):
        """
        Run the backtest.

        Parameters:
        -----------
        start_date : str or datetime
            Start date for the backtest
        end_date : str or datetime
            End date for the backtest
        rebalance_freq : str
            Frequency for rebalancing the portfolio (D, W, M)

        Returns:
        --------
        pd.DataFrame
            DataFrame with portfolio history
        """
        # Set default dates if not provided
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365 * 2)
        if end_date is None:
            end_date = datetime.now()

        # Convert to datetime if needed
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)

        logger.info(f"Starting backtest from {start_date} to {end_date}")

        # Generate date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # Business days
        rebalance_dates = pd.date_range(start=start_date, end=end_date, freq=rebalance_freq)

        # Get tickers from universe
        tickers = self.universe['ticker'].tolist()
        logger.info(f"Loading data for {len(tickers)} tickers")

        # Get historical data
        data = self.get_historical_data(tickers, period="5y")
        logger.info(f"Got data for {len(data)} tickers")

        # Preprocess data
        for ticker, df in data.items():
            if len(df) > self.atr_period:
                data[ticker] = self.calculate_atr(df)
                data[ticker] = self.calculate_all_time_high(data[ticker])
                data[ticker] = self.calculate_stop_price(data[ticker])

        # Run the simulation
        for current_date in date_range:
            current_date_str = current_date.strftime('%Y-%m-%d')

            # Filter data to current date
            current_data = {}
            for ticker, df in data.items():
                df_until_date = df[df.index <= current_date]
                if len(df_until_date) > 0:
                    current_data[ticker] = df_until_date

            # Update stops for existing positions
            self.update_stops(current_data, current_date)

            # Check for exit signals
            self.check_exits(current_data, current_date)

            # Rebalance on scheduled dates
            if current_date in rebalance_dates:
                logger.info(f"Rebalancing on {current_date_str}")
                target_weights = self.calculate_position_size(current_data, current_date)
                self.execute_trades(target_weights, current_data, current_date)

            # Update portfolio value
            self.update_portfolio_value(current_data, current_date)

        # Convert portfolio history to DataFrame
        portfolio_df = pd.DataFrame(self.portfolio_history)
        trades_df = pd.DataFrame(self.trades)

        # Calculate performance metrics
        self.calculate_performance_metrics(portfolio_df, trades_df)

        return portfolio_df, trades_df

    def calculate_performance_metrics(self, portfolio_df, trades_df):
        """
        Calculate performance metrics for the backtest.

        Parameters:
        -----------
        portfolio_df : pd.DataFrame
            DataFrame with portfolio history
        trades_df : pd.DataFrame
            DataFrame with trade history
        """
        if len(portfolio_df) < 2:
            logger.warning("Not enough data to calculate performance metrics")
            return

        # Calculate returns
        portfolio_df['daily_return'] = portfolio_df['portfolio_value'].pct_change()

        # Calculate cumulative return
        total_return = (portfolio_df['portfolio_value'].iloc[-1] / self.initial_capital) - 1

        # Calculate annualized return
        days = (portfolio_df['date'].iloc[-1] - portfolio_df['date'].iloc[0]).days
        years = days / 365
        annualized_return = (1 + total_return) ** (1 / years) - 1

        # Calculate volatility
        daily_std = portfolio_df['daily_return'].std()
        annualized_vol = daily_std * np.sqrt(252)

        # Calculate Sharpe ratio (assuming risk-free rate of 0 for simplicity)
        sharpe_ratio = annualized_return / annualized_vol if annualized_vol > 0 else 0

        # Calculate drawdowns
        portfolio_df['cum_return'] = (1 + portfolio_df['daily_return']).cumprod()
        portfolio_df['cum_return_max'] = portfolio_df['cum_return'].cummax()
        portfolio_df['drawdown'] = portfolio_df['cum_return'] / portfolio_df['cum_return_max'] - 1
        max_drawdown = portfolio_df['drawdown'].min()

        # Calculate trade statistics
        if len(trades_df) > 0:
            winning_trades = trades_df[trades_df['pnl'] > 0]
            losing_trades = trades_df[trades_df['pnl'] < 0]

            win_rate = len(winning_trades) / len(trades_df) if len(trades_df) > 0 else 0
            avg_win = winning_trades['pnl'].mean() if len(winning_trades) > 0 else 0
            avg_loss = losing_trades['pnl'].mean() if len(losing_trades) > 0 else 0
            profit_factor = abs(winning_trades['pnl'].sum() / losing_trades['pnl'].sum()) if len(losing_trades) > 0 and \
                                                                                             losing_trades[
                                                                                                 'pnl'].sum() != 0 else float(
                'inf')

            # Calculate holding period statistics
            avg_holding_period = trades_df['holding_days'].mean()

            # Analyze profit skew
            if len(trades_df) > 0:
                trades_df_sorted = trades_df.sort_values(by='pnl', ascending=False)
                trades_df_sorted['cum_pnl'] = trades_df_sorted['pnl'].cumsum()
                trades_df_sorted['pnl_pct_of_total'] = trades_df_sorted['cum_pnl'] / trades_df_sorted['pnl'].sum()

                # Find what percentage of trades generate what percentage of profits
                top_trades_pct = len(
                    trades_df_sorted[trades_df_sorted['cum_pnl'] <= 0.8 * trades_df_sorted['pnl'].sum()]) / len(
                    trades_df_sorted)

                logger.info(f"Top {top_trades_pct * 100:.1f}% of trades generate 80% of profits")

        # Log performance metrics
        logger.info("===== Performance Metrics =====")
        logger.info(f"Total Return: {total_return * 100:.2f}%")
        logger.info(f"Annualized Return: {annualized_return * 100:.2f}%")
        logger.info(f"Annualized Volatility: {annualized_vol * 100:.2f}%")
        logger.info(f"Sharpe Ratio: {sharpe_ratio:.2f}")
        logger.info(f"Max Drawdown: {max_drawdown * 100:.2f}%")

        if len(trades_df) > 0:
            logger.info("===== Trade Statistics =====")
            logger.info(f"Number of Trades: {len(trades_df)}")
            logger.info(f"Win Rate: {win_rate * 100:.2f}%")
            logger.info(f"Average Win: ${avg_win:.2f}")
            logger.info(f"Average Loss: ${avg_loss:.2f}")
            logger.info(f"Profit Factor: {profit_factor:.2f}")
            logger.info(f"Average Holding Period: {avg_holding_period:.2f} days")

        # Return the metrics
        self.performance_metrics = {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'annualized_vol': annualized_vol,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown
        }

        if len(trades_df) > 0:
            self.trade_metrics = {
                'num_trades': len(trades_df),
                'win_rate': win_rate,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'avg_holding_period': avg_holding_period
            }


# Example usage
if __name__ == "__main__":
    # Initialize the strategy
    strategy = TrendFollowingStrategy(
        initial_capital=1000000,
        target_volatility=0.15,
        max_leverage=2.0,
        min_price=10,
        min_adv_usd=1000000,
        atr_period=42,
        atr_multiplier=10,
        min_weight_change=0.005
    )

    # Run the backtest
    start_date = "2020-01-01"
    end_date = "2023-12-31"
    portfolio_history, trades = strategy.run_backtest(start_date, end_date, rebalance_freq='W-MON')

    # Save results
    portfolio_history.to_csv("portfolio_history.csv", index=False)
    trades.to_csv("trades.csv", index=False)

    # Generate a basic performance report
    print("\nPerformance Summary:")
    print(f"Initial Capital: ${strategy.initial_capital:,.2f}")
    print(f"Final Portfolio Value: ${strategy.portfolio_value:,.2f}")
    print(f"Total Return: {strategy.performance_metrics['total_return'] * 100:.2f}%")
    print(f"Annualized Return: {strategy.performance_metrics['annualized_return'] * 100:.2f}%")
    print(f"Sharpe Ratio: {strategy.performance_metrics['sharpe_ratio']:.2f}")
    print(f"Max Drawdown: {strategy.performance_metrics['max_drawdown'] * 100:.2