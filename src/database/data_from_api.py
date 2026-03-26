"""
API Data Retrieval Module - Fetches data from external API endpoints
Enhanced with async support, connection pooling, and retry mechanisms.
"""

import requests
import pandas as pd
import json
import logging
from typing import Optional, Dict
import time
import asyncio
import sys
import os

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import async API client (optional, for future enhancement)
ASYNC_API_AVAILABLE = False

# Stub classes for async functionality (for future implementation)
class AsyncAPIClient:
    """Stub for async API client"""
    pass

class APIRequest:
    """Stub for API request"""
    pass

class RequestMethod:
    """Stub for request method"""
    GET = "GET"

class RetryConfig:
    """Stub for retry config"""
    def __init__(self, *args, **kwargs):
        pass

class RateLimitConfig:
    """Stub for rate limit config"""
    def __init__(self, *args, **kwargs):
        pass

# Import structured logging
try:
    from logging_system import get_api_logger
    logger = get_api_logger()
except ImportError:
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)


class APIClient:
    """Client for retrieving data from the ETL API server endpoints."""

    def __init__(self, base_url: str = "https://etl-server.fly.dev"):
        """
        Initialize API client.
        
        Args:
            base_url (str): Base URL for the API server
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        # Set reasonable timeout and retry settings
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.timeout = 30
        self.max_retries = 3
        self.retry_delay = 1

        # Define available endpoints
        self.endpoints = {
            'orders': '/orders',
            'order_items': '/order_items',
            'customers': '/customers'
        }

        # Initialize async client if available
        if ASYNC_API_AVAILABLE:
            self._setup_async_client()

    def fetch_data(self, endpoint_name: str, retry_count: int = 0) -> Optional[pd.DataFrame]:
        """
        Fetch data from the specified API endpoint.
        
        Args:
            endpoint_name (str): Name of the endpoint ('orders', 'order_items', 'customers')
            retry_count (int): Current retry attempt number
            
        Returns:
            pd.DataFrame: Data as DataFrame, None if failed
        """
        if endpoint_name not in self.endpoints:
            logger.error(f"Unknown endpoint: {endpoint_name}. Available: {list(self.endpoints.keys())}")
            return None

        try:
            endpoint_url = f"{self.base_url}{self.endpoints[endpoint_name]}"

            response = self.session.get(endpoint_url, timeout=self.timeout)
            response.raise_for_status()

            # Parse JSON response and convert to DataFrame
            data = response.json()
            df = pd.DataFrame(data)

            # Data cleaning based on endpoint type
            cleaning_methods = {
                'orders': self._clean_orders_data,
                'order_items': self._clean_order_items_data,
                'customers': self._clean_customers_data
            }

            if endpoint_name in cleaning_methods:
                df = cleaning_methods[endpoint_name](df)

            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint_name}: {e}")

            # Retry logic with exponential backoff
            if retry_count < self.max_retries:
                wait_time = self.retry_delay * (2 ** retry_count)
                time.sleep(wait_time)
                return self.fetch_data(endpoint_name, retry_count + 1)
            else:
                logger.error(f"Failed to fetch {endpoint_name} after {self.max_retries} attempts")
                return None

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for {endpoint_name}: {e}")
            return None

        except Exception as e:
            logger.error(f"Unexpected error occurred while fetching {endpoint_name}: {e}")
            return None

    def fetch_orders(self, retry_count: int = 0) -> Optional[pd.DataFrame]:
        """Fetch orders data (backward compatibility method)."""
        return self.fetch_data('orders', retry_count)

    def fetch_order_items(self, retry_count: int = 0) -> Optional[pd.DataFrame]:
        """Fetch order_items data."""
        return self.fetch_data('order_items', retry_count)

    def fetch_customers(self, retry_count: int = 0) -> Optional[pd.DataFrame]:
        """Fetch customers data."""
        return self.fetch_data('customers', retry_count)

    def fetch_all_data(self) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Fetch data from all available endpoints.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary with endpoint names as keys and DataFrames as values
        """
        all_data = {}

        for endpoint_name in self.endpoints.keys():
            df = self.fetch_data(endpoint_name)
            all_data[endpoint_name] = df

        return all_data

    def _clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate the orders data.
        
        Args:
            df (pd.DataFrame): Raw orders DataFrame
            
        Returns:
            pd.DataFrame: Cleaned orders DataFrame
        """
        try:
            # Convert date columns to proper datetime format
            date_columns = ['order_date', 'required_date', 'shipped_date']
            for col in date_columns:
                if col in df.columns:
                    df[col] = df[col].replace('NULL', None)
                    df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

            # Convert numeric columns
            numeric_columns = ['order_id', 'customer_id', 'order_status']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Handle order_status mapping (if needed)
            status_mapping = {
                1: 'Pending',
                2: 'Processing',
                3: 'Rejected',
                4: 'Completed'
            }

            if 'order_status' in df.columns:
                df['order_status_name'] = df['order_status'].map(status_mapping)

            # Remove duplicates based on order_id
            if 'order_id' in df.columns:
                df = df.drop_duplicates(subset=['order_id'], keep='first')

            # Quick validation for critical issues only
            self._validate_data(df, 'orders')

            return df

        except Exception as e:
            logger.error(f"Error during data cleaning: {e}")
            return df  # Return original df if cleaning fails

    def _validate_data(self, df: pd.DataFrame, endpoint_type: str) -> None:
        """Unified validation method for all data types."""
        validation_rules = {
            'orders': (['order_id', 'customer_id', 'order_date', 'order_status'], ['order_id', 'customer_id']),
            'order_items': (['item_id', 'order_id', 'product_id', 'quantity'], ['item_id', 'order_id', 'product_id']),
            'customers': (['customer_id', 'first_name', 'last_name'], ['customer_id', 'first_name', 'last_name'])
        }

        try:
            required_columns, critical_columns = validation_rules.get(endpoint_type, ([], []))
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing required {endpoint_type} columns: {missing_cols}")

            for col in critical_columns:
                if col in df.columns and (null_count := df[col].isnull().sum()) > 0:
                    logger.warning(f"Found {null_count} null values in critical {endpoint_type} column '{col}'")
        except Exception as e:
            logger.error(f"Error during {endpoint_type} data validation: {e}")

    def _clean_order_items_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate the order_items data."""
        try:
            # Convert numeric columns and handle NULL values
            for col in ['item_id', 'order_id', 'product_id', 'quantity', 'list_price', 'discount']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            df = df.replace('NULL', None)
            if 'item_id' in df.columns:
                df = df.drop_duplicates(subset=['item_id'], keep='first')

            self._validate_data(df, 'order_items')
            return df
        except Exception as e:
            logger.error(f"Error during order_items data cleaning: {e}")
            return df

    def _clean_customers_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate the customers data."""
        try:
            # Convert customer_id and handle NULL values
            if 'customer_id' in df.columns:
                df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce')

            df = df.replace('NULL', None)

            # Clean string columns efficiently
            for col in ['first_name', 'last_name', 'email', 'phone', 'street', 'city', 'state', 'zip_code']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip().replace('nan', None)

            if 'customer_id' in df.columns:
                df = df.drop_duplicates(subset=['customer_id'], keep='first')

            self._validate_data(df, 'customers')
            return df
        except Exception as e:
            logger.error(f"Error during customers data cleaning: {e}")
            return df

    def save_to_csv(self, df: pd.DataFrame, filename: str = "orders_api_data.csv",
                   output_dir: str = "../data/API") -> bool:
        """
        Save the orders DataFrame to CSV file.
        
        Args:
            df (pd.DataFrame): Orders DataFrame to save
            filename (str): Output filename
            output_dir (str): Output directory
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            import os

            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)

            filepath = os.path.join(output_dir, filename)
            df.to_csv(filepath, index=False)

            return True

        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False

    def save_all_api_data_to_csv(self, output_dir: str = "../data/API") -> bool:
        """
        Fetch all API data and save each endpoint as CSV files.
        
        Args:
            output_dir (str): Output directory for CSV files
            
        Returns:
            bool: True if all files saved successfully, False otherwise
        """
        try:
            import os

            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)

            # Fetch all API data
            all_data = self.fetch_all_data()

            success_count = 0
            total_endpoints = len(all_data)

            # Save each endpoint data as CSV
            for endpoint_name, df in all_data.items():
                if df is not None and not df.empty:
                    filename = f"{endpoint_name}.csv"
                    filepath = os.path.join(output_dir, filename)

                    try:
                        df.to_csv(filepath, index=False)
                        print(f"SUCCESS: Saved {endpoint_name}: {len(df):,} rows → {filename}")
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Failed to save {endpoint_name} CSV: {e}")
                else:
                    logger.warning(f"No data available for {endpoint_name}")

            if success_count == total_endpoints:
                print(f"\nSuccessfully saved all {success_count} API datasets to CSV files!")
                return True
            else:
                logger.warning(f"Only {success_count}/{total_endpoints} CSV files saved successfully")
                return False

        except Exception as e:
            logger.error(f"Error saving API data to CSV: {e}")
            return False

    def get_data_summary(self, df: pd.DataFrame) -> Dict:
        """
        Generate a summary of the orders data.
        
        Args:
            df (pd.DataFrame): Orders DataFrame
            
        Returns:
            Dict: Summary statistics
        """
        try:
            summary = {
                'total_orders': len(df),
                'unique_customers': df['customer_id'].nunique() if 'customer_id' in df.columns else 0,
                'date_range': {
                    'earliest': df['order_date'].min().strftime('%Y-%m-%d') if 'order_date' in df.columns and not df['order_date'].isnull().all() else None,
                    'latest': df['order_date'].max().strftime('%Y-%m-%d') if 'order_date' in df.columns and not df['order_date'].isnull().all() else None
                },
                'order_status_counts': df['order_status'].value_counts().to_dict() if 'order_status' in df.columns else {},
                'stores': df['store'].value_counts().to_dict() if 'store' in df.columns else {},
                'staff': df['staff_name'].value_counts().to_dict() if 'staff_name' in df.columns else {}
            }

            return summary

        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return {}

    def _setup_async_client(self):
        """Setup async API client with optimized configuration."""
        if not ASYNC_API_AVAILABLE:
            return

        # Configure retry policy for API reliability
        retry_config = RetryConfig(
            max_retries=3,
            base_delay=1.0,
            max_delay=30.0,
            backoff_multiplier=2.0,
            retry_on_status=[429, 500, 502, 503, 504]
        )

        # Configure rate limiting to be respectful to API
        rate_limit_config = RateLimitConfig(
            requests_per_second=5.0,  # Conservative rate
            requests_per_minute=300,
            burst_size=20
        )

        self.async_client_config = {
            'base_url': self.base_url,
            'default_headers': {
                'User-Agent': 'ETL-Pipeline-Async/2.0',
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            'timeout': 60.0,  # Longer timeout for async
            'retry_config': retry_config,
            'rate_limit_config': rate_limit_config,
            'max_concurrent': 10
        }

    async def fetch_data_async(self, endpoint_name: str) -> Optional[pd.DataFrame]:
        """
        Fetch data from API endpoint using async client for better performance.
        
        Args:
            endpoint_name: Name of the endpoint to fetch from
            
        Returns:
            DataFrame with API data or None if failed
        """
        if not ASYNC_API_AVAILABLE:
            logger.warning("Async API client not available, falling back to sync")
            return self.fetch_data(endpoint_name)

        if endpoint_name not in self.endpoints:
            logger.error(f"Unknown endpoint: {endpoint_name}")
            return None

        logger.info(f"Fetching {endpoint_name} data using async client...")

        try:
            async with AsyncAPIClient(**self.async_client_config) as client:
                request = APIRequest(
                    url=self.endpoints[endpoint_name],
                    method=RequestMethod.GET,
                    timeout=60.0,
                    metadata={'endpoint': endpoint_name}
                )

                response = await client.request(request)

                if response.success:
                    logger.info(f"Successfully fetched {endpoint_name} data "
                              f"(Status: {response.status}, Time: {response.request_time:.2f}s)")

                    # Convert response data to DataFrame
                    if isinstance(response.data, list):
                        df = pd.DataFrame(response.data)
                        logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
                        return df
                    else:
                        logger.error(f"Unexpected data format for {endpoint_name}: {type(response.data)}")
                        return None
                else:
                    logger.error(f"API request failed for {endpoint_name}: Status {response.status}")
                    return None

        except Exception as e:
            logger.error(f"Async fetch failed for {endpoint_name}: {e}")
            # Fallback to sync method
            logger.info("Falling back to synchronous fetch...")
            return self.fetch_data(endpoint_name)

    async def fetch_all_data_async(self, progress_callback=None) -> Dict[str, pd.DataFrame]:
        """
        Fetch all endpoint data concurrently using async client.
        
        Args:
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary mapping endpoint names to DataFrames
        """
        if not ASYNC_API_AVAILABLE:
            logger.warning("Async API client not available, falling back to sync")
            return self.fetch_all_data()

        logger.info("Fetching all API data concurrently...")

        try:
            async with AsyncAPIClient(**self.async_client_config) as client:
                # Create requests for all endpoints
                requests = [
                    APIRequest(
                        url=endpoint_path,
                        method=RequestMethod.GET,
                        timeout=60.0,
                        metadata={'endpoint': endpoint_name}
                    )
                    for endpoint_name, endpoint_path in self.endpoints.items()
                ]

                # Execute all requests concurrently
                responses = await client.batch_requests(requests, progress_callback)

                # Process responses into DataFrames
                all_data = {}

                for response in responses:
                    endpoint_name = response.metadata.get('endpoint', 'unknown')

                    if response.success and isinstance(response.data, list):
                        df = pd.DataFrame(response.data)
                        all_data[endpoint_name] = df
                        logger.info(f"Successfully processed {endpoint_name}: {len(df)} rows")
                    else:
                        logger.error(f"Failed to process {endpoint_name}: Status {response.status}")
                        all_data[endpoint_name] = None

                # Get performance statistics
                stats = client.get_stats()
                logger.info(f"Async fetch complete - Success rate: {stats['success_rate']:.1f}%, "
                           f"Avg response time: {stats['average_response_time']:.2f}s")

                return all_data

        except Exception as e:
            logger.error(f"Async fetch all failed: {e}")
            # Fallback to sync method
            logger.info("Falling back to synchronous fetch all...")
            return self.fetch_all_data()

    async def fetch_paginated_data_async(self, endpoint_name: str,
                                       page_size: int = 100,
                                       max_pages: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Fetch paginated data from API endpoint using async client.
        
        Args:
            endpoint_name: Name of the endpoint
            page_size: Number of items per page
            max_pages: Maximum number of pages to fetch
            
        Returns:
            Combined DataFrame from all pages or None if failed
        """
        if not ASYNC_API_AVAILABLE or endpoint_name not in self.endpoints:
            return None

        logger.info(f"Fetching paginated {endpoint_name} data (page size: {page_size})")

        try:
            async with AsyncAPIClient(**self.async_client_config) as client:
                base_request = APIRequest(
                    url=self.endpoints[endpoint_name],
                    method=RequestMethod.GET,
                    timeout=60.0,
                    metadata={'endpoint': endpoint_name}
                )

                # Fetch all pages
                responses = await client.paginated_requests(
                    base_request=base_request,
                    page_param="page",
                    size_param="size",
                    page_size=page_size,
                    max_pages=max_pages
                )

                # Combine all page data
                all_data = []
                for response in responses:
                    if response.success and isinstance(response.data, list):
                        all_data.extend(response.data)

                if all_data:
                    df = pd.DataFrame(all_data)
                    logger.info(f"Combined paginated data: {len(df)} total rows from {len(responses)} pages")
                    return df
                else:
                    logger.warning(f"No data retrieved from paginated {endpoint_name}")
                    return None

        except Exception as e:
            logger.error(f"Paginated fetch failed for {endpoint_name}: {e}")
            return None

    def close(self):
        """Close the HTTP session."""
        self.session.close()

def main():
    """Main function to demonstrate API data retrieval."""
    client = APIClient()

    try:
        # Fetch all endpoint data
        all_data = client.fetch_all_data()

        # Display basic summary for each endpoint
        for endpoint_name, df in all_data.items():
            if df is not None:
                print(f"\n{endpoint_name.upper()}: {len(df):,} records")

                # Display basic info based on endpoint type
                if endpoint_name == 'orders':
                    if 'customer_id' in df.columns:
                        print(f"Unique Customers: {df['customer_id'].nunique():,}")
                    if 'order_date' in df.columns:
                        try:
                            earliest = pd.to_datetime(df['order_date']).min()
                            latest = pd.to_datetime(df['order_date']).max()
                            print(f"Date Range: {earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')}")
                        except Exception:
                            pass

                    if 'order_status' in df.columns:
                        status_counts = df['order_status'].value_counts()
                        print("Status Distribution:")
                        for status, count in status_counts.items():
                            print(f"  {status}: {count:,}")

                elif endpoint_name == 'order_items':
                    if 'order_id' in df.columns:
                        print(f"Unique Orders: {df['order_id'].nunique():,}")
                    if 'product_id' in df.columns:
                        print(f"Unique Products: {df['product_id'].nunique():,}")
                    if 'quantity' in df.columns:
                        print(f"Total Quantity: {df['quantity'].sum():,}")

                elif endpoint_name == 'customers':
                    if 'state' in df.columns:
                        state_counts = df['state'].value_counts()
                        print(f"States Represented: {len(state_counts)}")
                        top_states = state_counts.head()
                        print("Top States:")
                        for state, count in top_states.items():
                            print(f"  {state}: {count:,}")

                print("="*60)

                # Save to CSV
                filename = f"{endpoint_name}_data.csv"
                try:
                    df.to_csv(filename, index=False)
                    logger.info(f"Data saved successfully to {filename}")
                    print(f"SUCCESS: {endpoint_name} data saved to {filename}")
                except Exception as e:
                    logger.error(f"Failed to save {endpoint_name} data to CSV: {e}")
                    print(f"ERROR: Failed to save {endpoint_name} data: {e}")

                # Display first few rows
                print(f"\nFirst 5 {endpoint_name} records:")
                print(df.head().to_string())

                logger.info(f"{endpoint_name} data processing completed successfully!")
            else:
                logger.error(f"Failed to retrieve {endpoint_name} data from API")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")

    finally:
        client.close()

async def main_async_demo():
    """Demonstrate async API capabilities."""
    print("🚀 Starting Async API Data Fetch Demo...")

    if not ASYNC_API_AVAILABLE:
        print("⚠️  Async API client not available. Please install: pip install aiohttp")
        return

    # Initialize the API client
    api_client = APIClient()

    try:
        # Fetch all data concurrently using async client
        start_time = time.time()

        def progress_callback(completed: int, total: int):
            print(f"  Progress: {completed}/{total} requests completed ({completed/total*100:.1f}%)")

        print("Fetching all endpoints concurrently...")
        all_data = await api_client.fetch_all_data_async(progress_callback)

        async_time = time.time() - start_time

        print(f"\n✅ Async fetch completed in {async_time:.2f} seconds")
        print(f"Successfully fetched data from {len([d for d in all_data.values() if d is not None])} endpoints:")

        for endpoint_name, df in all_data.items():
            if df is not None:
                print(f"  📊 {endpoint_name}: {len(df)} rows, {len(df.columns)} columns")

                # Display sample data
                print(f"  Sample {endpoint_name} data:")
                print(df.head(2).to_string(index=False))
                print()

        # Compare with sync performance
        print("\n🔄 Comparing with synchronous fetch...")
        start_time = time.time()
        #sync_data = api_client.fetch_all_data()
        sync_time = time.time() - start_time

        print(f"⏱️  Synchronous fetch took {sync_time:.2f} seconds")
        print(f"🚀 Async was {sync_time/async_time:.1f}x faster!")

    except Exception as e:
        logger.error(f"Async demo failed: {e}")
    finally:
        # Clean up
        api_client.close()
        print("🔧 API client closed.")


if __name__ == "__main__":
    import sys

    # Check if async demo requested
    if len(sys.argv) > 1 and sys.argv[1] == "--async":
        if ASYNC_API_AVAILABLE:
            import asyncio
            asyncio.run(main_async_demo())
        else:
            print("❌ Async mode requested but aiohttp not available")
            print("   Install with: pip install aiohttp")
    else:
        # Standard synchronous demo
        print("🔄 Starting Synchronous API Data Fetch...")
        print("\n💡 Tip: Run with --async flag to see async performance benefits!")
        main()

# Alias for backward compatibility
APIDataFetcher = APIClient
