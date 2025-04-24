import yfinance as yf
import pandas as pd
import os
import hashlib
import json
from datetime import datetime, timedelta


class YahooFinanceCache:
    def __init__(self, cache_dir="finance_cache", cache_expiry_days=1):
        """Initialize the Yahoo Finance cache manager."""
        self.cache_dir = cache_dir
        self.cache_expiry_days = cache_expiry_days
        self.metadata_file = os.path.join(cache_dir, "cache_metadata.json")
        self._ensure_cache_dir_exists()
        self._load_metadata()

    def _ensure_cache_dir_exists(self):
        """Create the cache directory if it doesn't exist."""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def _load_metadata(self):
        """Load the cache metadata file if it exists, or create a new one."""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    self.metadata = json.load(f)
            except Exception as e:
                print(f"Error loading metadata file: {e}")
                self.metadata = {"hash_mappings": {}, "last_updated": datetime.now().isoformat()}
        else:
            self.metadata = {"hash_mappings": {}, "last_updated": datetime.now().isoformat()}

    def _save_metadata(self):
        """Save the metadata to the cache directory."""
        try:
            self.metadata["last_updated"] = datetime.now().isoformat()
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2)
        except Exception as e:
            print(f"Error saving metadata file: {e}")

    def _convert_period_to_dates(self, period):
        """Convert a period string to start and end date strings."""
        end_date = datetime.now()

        if period.endswith('d'):
            days = int(period[:-1])
            start_date = end_date - timedelta(days=days)
        elif period.endswith('mo'):
            months = int(period[:-2])
            start_date = end_date - timedelta(days=30 * months)
        elif period.endswith('y'):
            years = int(period[:-1])
            start_date = end_date - timedelta(days=365 * years)
        elif period == 'ytd':
            start_date = datetime(end_date.year, 1, 1)
        elif period == 'max':
            # Default to 20 years for 'max' - just for caching purposes
            start_date = end_date - timedelta(days=365 * 20)
        else:
            # Default fallback
            start_date = end_date - timedelta(days=365)

        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        return start_str, end_str

    def _generate_cache_path(self, tickers, start_str, end_str, interval):
        """
        Generate a unique cache path based on query parameters and update metadata.

        The filename is hashed using MD5 for several important reasons:
        - Consistent Length: Fixed-length regardless of input parameters
        - Valid Filenames: Converts all inputs to safe hexadecimal characters
        - Uniqueness: Creates a unique identifier for each parameter combination
        - Organization: Avoids unwieldy filenames
        - Security: Obscures the exact query parameters
        """
        # Convert list of tickers to string if necessary
        if isinstance(tickers, list):
            tickers_str = ','.join(tickers)
        else:
            tickers_str = tickers

        params = f"{tickers_str}_{start_str}_{end_str}_{interval}"
        hash_value = hashlib.md5(params.encode()).hexdigest()
        filename = hash_value + ".parquet"

        # Update metadata with this mapping
        self.metadata["hash_mappings"][hash_value] = {
            "tickers": tickers_str,
            "start_date": start_str,
            "end_date": end_str,
            "interval": interval,
            "created_at": datetime.now().isoformat(),
            "original_params": params
        }
        self._save_metadata()

        return os.path.join(self.cache_dir, filename)

    def _is_cache_valid(self, cache_path):
        """Check if the cache file exists and is not expired."""
        if not os.path.exists(cache_path):
            return False

        cache_modified_time = os.path.getmtime(cache_path)
        cache_age_days = (datetime.now().timestamp() - cache_modified_time) / (60 * 60 * 24)

        return cache_age_days <= self.cache_expiry_days

    def read_cache(self, cache_path):
        """Read data from cache."""
        try:
            df = pd.read_parquet(cache_path)
            print(f"Loaded data from cache: {cache_path}")
            return df
        except Exception as e:
            print(f"Error loading from cache: {e}")
            return None

    def write_cache(self, df, cache_path):
        """Write data to cache."""
        try:
            df.to_parquet(cache_path)
            print(f"Saved data to cache: {cache_path}")
            return True
        except Exception as e:
            print(f"Error saving to cache: {e}")
            return False

    def find_cache_by_params(self, tickers=None, start_date=None, end_date=None, interval=None):
        """Search the metadata for cache entries matching the given parameters."""
        results = []

        for hash_value, info in self.metadata["hash_mappings"].items():
            match = True

            if tickers is not None:
                tickers_str = tickers if isinstance(tickers, str) else ','.join(tickers)
                if info["tickers"] != tickers_str:
                    match = False
            if start_date is not None and info["start_date"] != start_date:
                match = False
            if end_date is not None and info["end_date"] != end_date:
                match = False
            if interval is not None and info["interval"] != interval:
                match = False

            if match:
                cache_path = os.path.join(self.cache_dir, hash_value + ".parquet")
                exists = os.path.exists(cache_path)

                results.append({
                    "hash": hash_value,
                    "file_path": cache_path,
                    "file_exists": exists,
                    "info": info
                })

        return results

    def clean_expired_cache(self):
        """Remove expired cache files and update metadata."""
        removed_count = 0

        for hash_value in list(self.metadata["hash_mappings"].keys()):
            cache_path = os.path.join(self.cache_dir, hash_value + ".parquet")

            if os.path.exists(cache_path):
                # Check if expired
                if not self._is_cache_valid(cache_path):
                    try:
                        os.remove(cache_path)
                        removed_count += 1
                        # Remove from metadata
                        del self.metadata["hash_mappings"][hash_value]
                    except Exception as e:
                        print(f"Error removing expired cache file {cache_path}: {e}")
            else:
                # Remove metadata for non-existent files
                del self.metadata["hash_mappings"][hash_value]
                removed_count += 1

        self._save_metadata()
        return removed_count


def get_yahoo_finance_data(cache, tickers, interval="1d", use_cache=True, **kwargs) -> pd.DataFrame:
    """
    Download and cache Yahoo Finance data with mutually exclusive time parameters.

    Parameters:
    tickers (str or list): Ticker symbol(s) to download
    interval (str): Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    use_cache (bool): Whether to use cached data
    cache_dir (str): Directory to store cached data
    cache_expiry_days (int): Days after which cache expires

    Time Range (specify exactly one option):
    **kwargs: Either:
        - period (str): Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        - start (str) AND end (str): Start and end dates in YYYY-MM-DD format

    Returns:
    pandas.DataFrame: Yahoo Finance data
    """
    # Validate time parameters
    has_period = 'period' in kwargs
    has_start_end = 'start' in kwargs and 'end' in kwargs

    if has_period and has_start_end:
        raise ValueError("Specify either 'period' OR ('start' and 'end'), not both")

    if not (has_period or has_start_end):
        # Default to period='1y' if neither is specified
        has_period = True
        kwargs['period'] = '1y'

    if has_start_end and ('start' not in kwargs or 'end' not in kwargs):
        raise ValueError("If using date range, both 'start' and 'end' must be specified")

    # Determine start/end dates for caching
    if has_period:
        period = kwargs['period']
        start_str, end_str = cache._convert_period_to_dates(period)
    else:
        start_str = kwargs['start']
        end_str = kwargs['end']

    # Get cache path
    cache_path = cache._generate_cache_path(tickers, start_str, end_str, interval)

    # Try to load from cache
    if use_cache and cache._is_cache_valid(cache_path):
        cached_data = cache.read_cache(cache_path)
        if cached_data is not None:
            return cached_data

    # Download fresh data
    print(f"Downloading fresh data for {tickers}")
    if has_period:
        df = yf.download(tickers, period=kwargs['period'], interval=interval)
    else:
        df = yf.download(tickers, start=start_str, end=end_str, interval=interval)

    # Flatten multi-index columns to single level
    df = df.stack(level=1, future_stack=True).rename_axis(['Date', 'Ticker']).reset_index(level=1)

    # Lower-case all column names
    df.columns = [col.lower() for col in df.columns]

    # Save to cache if data was successfully downloaded
    if not df.empty and use_cache:
        cache.write_cache(df, cache_path)

    return df


# Example usage
if __name__ == "__main__":
    # Create cache manager
    cache = YahooFinanceCache(cache_dir="finance_cache", cache_expiry_days=7)

    # Clean any expired cache entries
    removed = cache.clean_expired_cache()
    if removed > 0:
        print(f"Removed {removed} expired cache entries")

    # Get data with default period (1y)
    default_data = get_yahoo_finance_data("AAPL", cache_dir="finance_cache")

    # Get data with specific period
    period_data = get_yahoo_finance_data("MSFT", period="2y", cache_dir="finance_cache")

    # Get data with date range
    date_range_data = get_yahoo_finance_data(
        ["GOOG", "AMZN"],
        start="2020-01-01",
        end="2021-12-31",
        cache_dir="finance_cache"
    )

    # Example of finding cache entries
    apple_entries = cache.find_cache_by_params(tickers="AAPL")
    print(f"Found {len(apple_entries)} cache entries for AAPL")

    # Display metadata structure
    print("\nMetadata structure:")
    print(json.dumps(list(cache.metadata["hash_mappings"].values())[0] if cache.metadata["hash_mappings"] else {},
                     indent=2))
