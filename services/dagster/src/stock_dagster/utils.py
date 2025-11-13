import polars as pl
import datetime

from duckdb import DuckDBPyConnection
from stock_dagster.defs.resources import DuckDBS3Resource
from duckdb import HTTPException


def build_date_intervals_df(start_date: datetime.date, end_date: datetime.date, interval: str = '3mo'):
    """Generates a DataFrame by splitting a date interval into smaller start/end chunks.
    Args:
        interval: polars interval string representation
     """
    return pl.select(start=pl.datetime_range(start=start_date,
                                             end=end_date, interval=interval,
                                             eager=True)
                     ).with_columns(start=pl.col('start').dt.date(),
                                    end=pl.col('start').shift(-1).dt.date() - datetime.timedelta(
                                        days=1)).fill_null(
        datetime.datetime.today().date())



