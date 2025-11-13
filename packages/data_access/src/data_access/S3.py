import datetime
import os
import duckdb
import polars as pl
from dotenv import load_dotenv
from duckdb import HTTPException
from data_access.validators import validate_isin, parse_date

load_dotenv()


class DuckS3:
    """Reading/Writing S3 files via DuckDB"""

    def __init__(self, bucket: str = None):
        if not bucket:
            bucket = os.environ.get('S3_BUCKET')
        self.s3 = f"s3://{bucket}"

        self.is_minio = os.getenv('IS_MINIO')

        self.filter_date_isin = {
            'date_from': {'column': 'date', 'operator': '>='},
            'date_to': {'column': 'date', 'operator': '<='},
            'isin': {'column': 'isin', 'operator': '='}
        }

    def file_exists(self, path) -> bool:
        """Checks if the given path exists"""

        try:
            with self.get_connection() as conn:
                conn.sql(f"""SELECT 1 FROM '{self.s3}/{path}'""")
                return True
        except HTTPException as e:
            if '404' in str(e):
                return False
            else:
                raise

    def get_connection(self, read_only: bool = True):
        """Gets connection to duckdb"""
        conn = duckdb.connect(":memory:")
        conn.sql("SET TimeZone = 'Europe/Warsaw'")
        if self.is_minio:
            conn.execute(f"""
                CREATE SECRET minio_secret (
                    TYPE S3,
                    KEY_ID '{os.getenv('S3_ACCESS_KEY_ID')}',
                    SECRET '{os.getenv('S3_SECRET_ACCESS_KEY')}',
                    ENDPOINT '{os.getenv('S3_ENDPOINT')}',
                    URL_STYLE '{os.getenv('S3_URL_STYLE', 'path')}',
                    USE_SSL {os.getenv('S3_USE_SSL', 'false')}
                )
            """)
        return conn

    def _query_filter(self, filter_def: dict, **kwargs) -> tuple[str, dict]:
        """
        Generates a SQL WHERE clause and corresponding parameters based on filter definitions and keyword arguments.

        This method processes a set of keyword arguments against a provided filter definition dictionary to construct
        a SQL WHERE clause with parameterized values. It validates that each provided parameter exists in the filter
        definition and builds the appropriate SQL conditions using the specified column names and operators.

        Args:
            filter_def: Dictionary mapping parameter names to their SQL filter definitions containing 'column' and 'operator' keys
            **kwargs: Keyword arguments representing filter parameters to be applied

        Returns:
            Tuple containing the constructed WHERE clause string and a dictionary of parameter values
        """
        conditions = []
        params = {}
        for param_name, value in kwargs.items():
            if value is None:
                continue
            if param_name not in filter_def.keys():
                raise ValueError(
                    f"'{param_name}' is not a valid filter parameter. Valid parameters are {list(filter_def.keys())}")

            col_name = filter_def[param_name]['column']
            operator = filter_def[param_name]['operator']
            param_key = f"param_{param_name}"
            where_statement = f"{col_name} {operator} ${param_key}" if operator.upper() != 'IN' else f"${param_key} IN {col_name}"
            conditions.append(where_statement)
            params[param_key] = value

        where_clausule = " AND ".join(conditions) if conditions else ""
        return where_clausule, params

    def write_data(self, data: pl.DataFrame | dict, path: str):
        """Write data (json/parquet) to given path in S3"""

        with self.get_connection(read_only=False) as conn:
            conn.sql(f"""COPY data TO '{self.s3 + path}'""")

    def read_file(self, path, hive: bool = False, file_name: bool = False, filter_query: str = None,
                  columns: list[str] = None) -> pl.DataFrame:
        """Reads parquet file from s3"""
        with self.get_connection() as conn:
            rel = conn.read_parquet(f"{self.s3}/{path}", hive_partitioning=hive, filename=file_name)
            if filter_query:
                rel = rel.filter(filter_query)

            if columns:
                rel = rel.select(*columns)

            return rel.pl()

    def get_news(self, isin: str = None, only_isin: bool = False, date_from: str = None, date_to: str = None,
                 source: str = None) -> pl.DataFrame:
        """
        Retrieves news data based on specified filters and date range.

        Args:
            isin: International Securities Identification Number to filter news by.
            only_isin: If True, only returns news related to identified companies.
            date_from: Start date for filtering news, formatted as string.
            date_to: End date for filtering news, formatted as string.
            source: News source to filter by, e.g., 'interia' or 'bankier'.

        Returns:
            pl.DataFrame: DataFrame containing news data with columns including title, link, date, summary,
            company_isins, year, month, and day.
        """
        news_filter = self.filter_date_isin.copy()
        news_filter['isin'] = {'column': 'company_isins', 'operator': 'IN'}
        date_from = parse_date(date_from)
        date_to = parse_date(date_to)
        validate_isin(isin)

        if source in ['interia', 'bankier']:
            source = f"{source.title()}Source"
        else:
            source = '*'

        path = f'{self.s3}/news/**/{source}.parquet'

        where, params = self._query_filter(filter_def=news_filter, date_from=date_from, date_to=date_to,
                                           isin=isin)

        query = f"""WITH data AS (SELECT title, link, MAKE_DATE(year, month, day) as _date, date, summary, company_isins 
        FROM read_parquet('{path}', hive_partitioning=True)
        ) 
        SELECT DISTINCT ON (link) title, link, date, summary, company_isins FROM data
                    {'WHERE ' + where if where else ""}
                    ORDER BY _date DESC"""
        with self.get_connection() as conn:
            statement = conn.sql(query, params=params)
            df = statement.pl()
            return df

    def get_today_news(self, company_isin=None, source: str = None) -> pl.DataFrame:
        """return latest news for today from S3 bucket"""
        today = datetime.date.today().isoformat()
        return self.get_news(isin=company_isin, date_from=today, source=source)

    def get_latest_isins(self):
        """return latest ISINs from companies_metadata parquet file in S3 bucket"""
        with self.get_connection() as conn:
            path = f'{self.s3}/companies_metadata/*.parquet'

            isins = conn.sql("""WITH metadata AS (SELECT *
                                                  FROM read_parquet('s3://stock/companies_metadata/*.parquet'))
                                SELECT company_isin
                                FROM metadata
                                WHERE date = (SELECT MAX(date) FROM metadata)

                             """).fetchall()

            return [x[0] for x in isins]

    def get_companies_metadata(self, isin: str | None = None) -> pl.DataFrame:
        """Retrieves company metadata from the latest available parquet file in S3."""
        validate_isin(isin)
        path = f'{self.s3}/companies_metadata/*.parquet'
        with self.get_connection() as conn:
            metadata = conn.sql(f"""WITH metadata AS (
                                    SELECT * FROM read_parquet('{path}')
                                    )
                                    SELECT * FROM metadata WHERE date = (SELECT MAX(date) FROM metadata)
            """)

            if isin:
                metadata = metadata.filter(f"company_isin = '{isin}'").select("EXCLUDE(day, month, year)")

            return metadata.pl()

    def aggregate_ohlc_daily(self, isin: str = None,
                             date_from: str | None = None, date_to: str | None = None,
                             ):
        """
        Aggregates daily OHLC (Open, High, Low, Close) data for financial instruments.

        This method retrieves and processes daily OHLC data from Parquet files stored in S3. It combines
        data from both the main OHLC daily dataset and a seed dataset, depending on the specified date range.
        The method supports filtering by ISIN and date range, and returns aggregated data grouped by date and ISIN.

        Args:
            isin: International Securities Identification Number to filter data. If None, all ISINs are included.
            date_from: Start date for filtering data. If None, no start date filter is applied.
            date_to: End date for filtering data. If None, no end date filter is applied.

        Returns:
            Polars DataFrame containing aggregated OHLC data with the following columns:
                - date: Trading date
                - isin: International Securities Identification Number
                - open: Opening price
                - close: Closing price
                - low: Lowest price
                - high: Highest price
                - volume: Total trading volume

        Raises:
            ValidationError: If the provided ISIN is invalid.
        """
        validate_isin(isin)
        date_from = parse_date(date_from)
        date_to = parse_date(date_to)

        with self.get_connection() as conn:
            ohlc_daily = conn.read_parquet(
                f"{self.s3}/ohlc/**/*.parquet",
                hive_partitioning=True
            )
            ohlc_seed = conn.read_parquet(
                f"{self.s3}/ohlc_seed/*.parquet"
            )
            seed_date = conn.sql("""
                                 SELECT CAST(split(filename, '/')[-1]::VARCHAR AS DATE)
                                 FROM read_parquet(?, filename = True)
                                 LIMIT 1
                                 """, params=[f"{self.s3}/ohlc_seed/*.parquet"]).fetchone()[0]
            use_seed = date_from is None or seed_date >= date_from

            where, params = self._query_filter(
                filter_def=self.filter_date_isin,
                date_from=date_from,
                date_to=date_to
            )
            where_clause = f"WHERE {where}" if where else ""

            query_parts = [
                f"""WITH data AS(
                SELECT *, MAKE_DATE(year, month, day) as date FROM ohlc_daily)
                    SELECT 
                        date, 
                        isin,
                        ARG_MIN(price, datetime) as open,  
                        ARG_MAX(price, datetime) as close, 
                        MIN(price) as low,
                        MAX(price) as high,
                        SUM(volume) as volume
                    FROM data 
                    {where_clause}
                    GROUP BY isin, date
                    """
            ]

            if use_seed:
                query_parts.append(f"""
                    UNION ALL
                    SELECT 
                        CAST(datetime AS DATE) as date,
                        isin,
                        open, close, low, high, volume
                    FROM ohlc_seed
                    {where_clause}
                    """)

            query_parts.append("ORDER BY isin, date")

            final_query = "\n".join(query_parts)

            result = conn.sql(final_query, params=params)
            return result.pl()

    def get_ohlc_minutely(self, isin: str = None, date_from: str | None = None, date_to: str | None = None):
        """
        Retrieves raw OHLC (Open, High, Low, Close) data for a specified ISIN within a date range.

        This method queries parquet files stored in S3 to fetch OHLC data. It supports filtering by ISIN and date range.
        The data is processed to include a proper date column constructed from year, month, and day fields.

        Args:
            isin: International Securities Identification Number to filter data. If None, no ISIN filtering is applied.
            date_from: Start date for filtering data in string format. If None, no start date filtering is applied.
            date_to: End date for filtering data in string format. If None, no end date filtering is applied.

        Returns:
            Query result containing OHLC data with additional date column constructed from year, month, and day fields.
        """
        validate_isin(isin)
        date_from = parse_date(date_from)
        date_to = parse_date(date_to)

        where, params = self._query_filter(filter_def=self.filter_date_isin, date_from=date_from, date_to=date_to)
        with self.get_connection() as conn:
            ohlc = conn.sql(
                f"""WITH data as (SELECT *, MAKE_DATE(year, month, day) as date
                   FROM read_parquet('{self.s3}/ohlc/**/*.parquet', hive_partitioning = True,
                                     filename = True))
                        SELECT * FROM data
                       {' WHERE ' + where if where else ""}""", params=params)
            return ohlc.pl()

    def _filter_df_by_date(self, date_from: str, date_to: str, df: pl.DataFrame,
                           col_date: str = 'date') -> pl.DataFrame:
        """helper function to filter a dataframe by date range"""
        date_from = parse_date(date_from)
        date_to = parse_date(date_to)
        if date_from:
            df = df.filter(pl.col(col_date) >= date_from)
        if date_to:
            df = df.filter(pl.col(col_date) <= date_to)
        return df

    def get_currencies(self, currency_type: str, date_from: str | None = None, date_to: str | None = None,
                       currency_code: str = None, columns: list[str] = None) -> pl.DataFrame:
        """
        Retrieves currency data from parquet files with optional filtering by date range and currency code.

        This method reads currency data from parquet files based on the specified currency type and applies
        filtering operations to narrow down the results according to the provided parameters.

        Args:
            currency_type: Type of currency data to retrieve (e.g., 'exchange_rates', 'currency_codes')
            date_from: Start date for filtering records (inclusive), format 'YYYY-MM-DD'
            date_to: End date for filtering records (inclusive), format 'YYYY-MM-DD'
            currency_code: Specific currency code to filter results by (e.g., 'USD', 'EUR')
            columns: List of column names to include in the result DataFrame, None means all columns

        Returns:
            pl.DataFrame: DataFrame containing filtered currency data with specified columns
        """
        df = self.read_file(f"currencies/{currency_type}.parquet", columns=columns)
        df = self._filter_df_by_date(date_from, date_to, df, 'effective_date')

        if currency_code:
            df = df.filter(pl.col('code') == currency_code)

        return df

    def get_gold_prices(self, date_from: str | None = None, date_to: str | None = None) -> pl.DataFrame:
        """
        Retrieves gold prices data from a parquet file and optionally filters it by date range.

        This method reads gold prices data from a parquet file, converts the date column to proper date type,
        and applies date filtering if specified. The method handles both individual date filtering and
        combined date range filtering.

        Args:
            date_from: Start date for filtering records. If None, no start date filtering is applied.
            date_to: End date for filtering records. If None, no end date filtering is applied.

        Returns:
            pl.DataFrame: DataFrame containing gold prices data with date column cast to Date type.
                If date filtering is applied, returns filtered DataFrame based on the specified date range.
        """
        df = self.read_file("gold_prices/gold_prices.parquet")
        df = df.with_columns(pl.col('date').cast(pl.Date))
        if date_from or date_to:
            df = self._filter_df_by_date(date_from, date_to, df)
        return df

    def get_llm_summary(self, date_from: str | None = None, date_to: str | None = None):
        """Retrieves last LLM summary for data"""
        with self.get_connection() as conn:
            return (conn.read_parquet(f"{self.s3}/llm_summaries/**/*.parquet")
                    .order('date DESC')
                    .fetchone())
