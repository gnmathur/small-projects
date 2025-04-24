"""
US M2 Money Supply Year-over-Year Change Analysis

This script:
1. Downloads M2 money supply data from FRED (Federal Reserve Economic Data)
2. Calculates year-over-year percentage changes
3. Creates advanced visualizations of the results using Plotly
4. Saves the data to a CSV file
5. Allows fetching all available historical M2 data
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import requests
from datetime import datetime, timedelta
import os
import argparse

class M2MoneySupplyAnalyzer:
    def __init__(self, api_key=None):
        """
        Initialize the M2 Money Supply analyzer.

        Args:
            api_key (str, optional): FRED API key. If None, the script will look for
                                     it in environment variables as FRED_API_KEY.
        """
        # Get API key from environment variables if not provided
        self.api_key = api_key or os.environ.get('FRED_API_KEY')
        if not self.api_key:
            print("Warning: No FRED API key provided. You can get one at https://fred.stlouisfed.org/docs/api/api_key.html")
            print("Using alternative data source for demo purposes...")

        self.base_url = "https://api.stlouisfed.org/fred/series/observations"
        self.m2_series_id = "M2SL"  # M2 Money Stock, Seasonally Adjusted
        self.data = None

        # Define historical recession periods for visualization
        self.recessions = [
            {'name': '1970 Recession', 'start': '1969-12-01', 'end': '1970-11-30'},
            {'name': '1973-75 Recession', 'start': '1973-11-01', 'end': '1975-03-31'},
            {'name': '1980 Recession', 'start': '1980-01-01', 'end': '1980-07-31'},
            {'name': '1981-82 Recession', 'start': '1981-07-01', 'end': '1982-11-30'},
            {'name': '1990-91 Recession', 'start': '1990-07-01', 'end': '1991-03-31'},
            {'name': '2001 Recession', 'start': '2001-03-01', 'end': '2001-11-30'},
            {'name': '2008 Financial Crisis', 'start': '2007-12-01', 'end': '2009-06-30'},
            {'name': 'COVID-19 Pandemic', 'start': '2020-02-01', 'end': '2020-04-30'}
        ]

    def fetch_data(self, start_date=None, end_date=None, all_available=False):
        """
        Fetch M2 money supply data from FRED.

        Args:
            start_date (str, optional): Start date in format 'YYYY-MM-DD'.
            end_date (str, optional): End date in format 'YYYY-MM-DD'.
            all_available (bool, optional): If True, fetches all available historical data.

        Returns:
            pandas.DataFrame: DataFrame containing the M2 data.
        """
        # Set default dates if not provided
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        if all_available:
            # M2 series began in January 1959
            start_date = "1959-01-01"
            print(f"Fetching all available M2 data (from {start_date} to {end_date})")
        elif start_date is None:
            # Get at least 5 years of data for good YoY analysis
            start_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')

        # If we have an API key, fetch from FRED
        if self.api_key:
            params = {
                'series_id': self.m2_series_id,
                'api_key': self.api_key,
                'file_type': 'json',
                'observation_start': start_date,
                'observation_end': end_date,
                'frequency': 'm',  # Monthly frequency
                'units': 'lin',    # Levels, not percent changes
            }

            response = requests.get(self.base_url, params=params)

            if response.status_code == 200:
                data = response.json()
                observations = data['observations']

                # Convert to DataFrame
                df = pd.DataFrame(observations)
                df['date'] = pd.to_datetime(df['date'])
                df['value'] = pd.to_numeric(df['value'])

                self.data = df[['date', 'value']].rename(columns={'value': 'm2_value'})
                print(f"Successfully downloaded {len(self.data)} records from FRED.")
                return self.data
            else:
                print(f"Error fetching data from FRED: {response.status_code}")
                print(response.text)
                return self._use_fallback_data(start_date, end_date)
        else:
            # Use fallback data for demo purposes
            return self._use_fallback_data(start_date, end_date)

    def _use_fallback_data(self, start_date, end_date):
        """
        Use alternative data source if FRED API is not available.
        For demo purposes, we'll fetch data from a CSV file or generate dummy data.

        In a real application, you might want to:
        1. Use another API source
        2. Scrape from a public website (where permitted)
        3. Load from a local cache
        """
        try:
            # Try to load data from Federal Reserve Bank of St. Louis directly
            url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=M2SL"
            df = pd.read_csv(url)
            df.columns = ['date', 'm2_value']
            df['date'] = pd.to_datetime(df['date'])

            # Filter by date range
            mask = (df['date'] >= pd.to_datetime(start_date)) & (df['date'] <= pd.to_datetime(end_date))
            self.data = df.loc[mask].copy()

            print(f"Successfully downloaded {len(self.data)} records from FRED CSV.")
            return self.data
        except:
            print("Could not fetch data from alternative source. Generating synthetic data for demonstration.")
            # Generate synthetic data for demonstration
            date_range = pd.date_range(start=start_date, end=end_date, freq='M')
            values = [15000 + i * 100 + (i**2) for i in range(len(date_range))]  # Synthetic growth pattern

            self.data = pd.DataFrame({
                'date': date_range,
                'm2_value': values
            })
            print(f"Generated {len(self.data)} synthetic records for demonstration.")
            return self.data

    def calculate_yoy_change(self):
        """
        Calculate year-over-year percentage change in M2 money supply.

        Returns:
            pandas.DataFrame: DataFrame with original data and YoY change.
        """
        if self.data is None:
            print("No data available. Please fetch data first.")
            return None

        # Create a copy to avoid modifying the original
        df = self.data.copy()

        # Sort by date to ensure correct calculations
        df = df.sort_values('date')

        # Calculate year-over-year percentage change
        df['m2_yoy_pct'] = df['m2_value'].pct_change(periods=12) * 100

        return df

    def visualize_data(self, data=None, save_path=None, interactive=True):
        """
        Create an interactive visualization of the M2 YoY changes using Plotly.

        Args:
            data (pandas.DataFrame, optional): Data to visualize. If None, uses calculate_yoy_change().
            save_path (str, optional): Path to save the visualization. If None, only displays.
            interactive (bool, optional): If True, creates interactive Plotly chart. If False, uses static matplotlib.
        """
        if data is None:
            data = self.calculate_yoy_change()

        if data is None or data.empty:
            print("No data to visualize.")
            return

        # Drop rows with NaN YoY values (first 12 months)
        plot_data = data.dropna(subset=['m2_yoy_pct'])

        if interactive:
            # Create interactive Plotly figure with two subplots
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=('M2 Money Supply (Seasonally Adjusted)',
                                'M2 Money Supply Year-over-Year % Change')
            )

            # Add M2 Money Supply trace
            fig.add_trace(
                go.Scatter(
                    x=data['date'],
                    y=data['m2_value'],
                    mode='lines',
                    name='M2 Money Supply',
                    line=dict(color='royalblue', width=2)
                ),
                row=1, col=1
            )

            # Add YoY Percentage Change trace
            fig.add_trace(
                go.Scatter(
                    x=plot_data['date'],
                    y=plot_data['m2_yoy_pct'],
                    mode='lines',
                    name='YoY % Change',
                    line=dict(color='green', width=2)
                ),
                row=2, col=1
            )

            # Add zero line for YoY chart
            fig.add_shape(
                type="line",
                x0=plot_data['date'].min(),
                y0=0,
                x1=plot_data['date'].max(),
                y1=0,
                line=dict(color="red", width=1, dash="dash"),
                row=2, col=1
            )

            # Add recession bands from historical data
            for recession in self.recessions:
                for row in [1, 2]:
                    fig.add_shape(
                        type="rect",
                        x0=recession['start'],
                        y0=0,
                        x1=recession['end'],
                        y1=1,
                        fillcolor="rgba(220,220,220,0.3)",
                        line=dict(width=0),
                        yref="paper",
                        row=row, col=1
                    )

            # Update layout
            fig.update_layout(
                height=800,
                width=1000,
                title_text="US M2 Money Supply Analysis",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                template="plotly_white"
            )

            # Format y-axis for M2 values
            fig.update_yaxes(
                title_text="Billions of Dollars",
                tickprefix="$",
                row=1, col=1
            )

            # Format y-axis for YoY percentage
            fig.update_yaxes(
                title_text="Percent Change (%)",
                ticksuffix="%",
                row=2, col=1
            )

            # Add range slider
            fig.update_layout(
                xaxis2=dict(
                    rangeselector=dict(
                        buttons=list([
                            dict(count=1, label="1m", step="month", stepmode="backward"),
                            dict(count=6, label="6m", step="month", stepmode="backward"),
                            dict(count=1, label="YTD", step="year", stepmode="todate"),
                            dict(count=1, label="1y", step="year", stepmode="backward"),
                            dict(count=5, label="5y", step="year", stepmode="backward"),
                            dict(count=10, label="10y", step="year", stepmode="backward"),
                            dict(step="all")
                        ])
                    ),
                    rangeslider=dict(visible=True),
                    type="date"
                )
            )

            # Add annotations for significant events (example)
            annotations = [
                dict(
                    x="2020-03-15",
                    y=27,  # Adjust based on actual data
                    xref="x2",
                    yref="y2",
                    text="Fed COVID Response",
                    showarrow=True,
                    arrowhead=2,
                    ax=0,
                    ay=-40
                )
            ]
            fig.update_layout(annotations=annotations)

            # Save if path provided
            if save_path:
                if save_path.endswith('.png') or save_path.endswith('.jpg'):
                    base_path = save_path.rsplit('.', 1)[0]
                    html_path = f"{base_path}.html"
                else:
                    html_path = f"{save_path}.html"

                fig.write_html(html_path)
                print(f"Interactive visualization saved to {html_path}")

                # Also save static image if requested
                if save_path.endswith('.png') or save_path.endswith('.jpg'):
                    fig.write_image(save_path)
                    print(f"Static image saved to {save_path}")

            # Display the figure
            fig.show()

            return fig

        else:
            # Fallback to matplotlib for static visualization
            import matplotlib.pyplot as plt

            # Create figure with two subplots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

            # Plot M2 Money Supply
            ax1.plot(data['date'], data['m2_value'], 'b-')
            ax1.set_title('M2 Money Supply (Seasonally Adjusted)')
            ax1.set_ylabel('Billions of Dollars')
            ax1.grid(True)

            # Format y-axis to show billions
            ax1.yaxis.set_major_formatter(lambda x, pos: f'${x/1000:.1f}T' if x >= 1000 else f'${x:.0f}B')

            # Plot YoY Percentage Change
            ax2.plot(plot_data['date'], plot_data['m2_yoy_pct'], 'g-')
            ax2.axhline(y=0, color='r', linestyle='-', alpha=0.3)
            ax2.set_title('M2 Money Supply Year-over-Year % Change')
            ax2.set_xlabel('Date')
            ax2.set_ylabel('Percent Change (%)')
            ax2.grid(True)

            # Format dates on x-axis
            plt.xticks(rotation=45)

            plt.tight_layout()

            if save_path:
                plt.savefig(save_path)
                print(f"Visualization saved to {save_path}")

            plt.show()

            return fig

    def save_to_csv(self, path):
        """
        Save the processed data to a CSV file.

        Args:
            path (str): Path to save the CSV file.
        """
        data = self.calculate_yoy_change()
        if data is not None:
            data.to_csv(path, index=False)
            print(f"Data saved to {path}")
        else:
            print("No data to save.")

def main():
    """
    Main function to run the M2 Money Supply analysis.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='M2 Money Supply Analysis Tool')
    parser.add_argument('--all-data', action='store_true',
                        help='Fetch all available historical M2 data')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Start date for analysis (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None,
                        help='End date for analysis (YYYY-MM-DD)')
    parser.add_argument('--static', action='store_true',
                        help='Use static matplotlib charts instead of interactive Plotly')
    parser.add_argument('--output', type=str, default="m2_analysis",
                        help='Base name for output files (without extension)')
    parser.add_argument('--api-key', type=str, default=None,
                        help='FRED API key (optional)')

    args = parser.parse_args()

    print("M2 Money Supply Year-over-Year Analysis")
    print("=======================================")

    # Initialize analyzer
    analyzer = M2MoneySupplyAnalyzer(api_key=args.api_key)

    # Determine date range
    if args.all_data:
        analyzer.fetch_data(all_available=True)
    else:
        start_date = args.start_date
        if start_date is None and not args.all_data:
            # Default to 10 years if not specified
            start_date = (datetime.now() - timedelta(days=10*365)).strftime('%Y-%m-%d')

        analyzer.fetch_data(start_date=start_date, end_date=args.end_date)

    # Calculate YoY changes
    data = analyzer.calculate_yoy_change()

    if data is not None:
        # Print recent statistics
        recent_data = data.tail(13).copy()  # Last year plus current month
        recent_data['date'] = recent_data['date'].dt.strftime('%Y-%m')

        print("\nRecent M2 Money Supply and YoY Changes:")
        print("----------------------------------------")
        print(recent_data[['date', 'm2_value', 'm2_yoy_pct']].to_string(index=False))

        # File paths
        viz_path = f"{args.output}.png"
        csv_path = f"{args.output}.csv"

        # Create visualization
        analyzer.visualize_data(
            data,
            save_path=viz_path,
            interactive=not args.static
        )

        # Save data
        analyzer.save_to_csv(csv_path)

        print("\nAnalysis complete!")
        print(f"Data saved to: {csv_path}")
        print(f"Visualization saved to: {viz_path}" + (" and " + args.output + ".html" if not args.static else ""))

if __name__ == "__main__":
    main()
