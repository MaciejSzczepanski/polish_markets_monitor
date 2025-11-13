import dagster as dg
from stock_dagster.defs.resources import DuckDBS3Resource
import datetime
from data_sources.sources.gpw.client import GpwSource
import polars as pl
from stock_dagster.config import API_RETRY_POLICY


@dg.asset(retry_policy=API_RETRY_POLICY)
def wig20_companies_metadata(context: dg.AssetExecutionContext, ducks3: DuckDBS3Resource) -> None:
    """Metadata of current WIG20 companies"""
    gpw = GpwSource()
    client = ducks3.get_resource()
    comapnies_isin = gpw.fetch_all_wig20_isin()
    companies_metadata = [gpw.fetch_metadata(x) for x in comapnies_isin]
    companies_metadata = pl.DataFrame(companies_metadata)
    now = datetime.datetime.now().isoformat()
    path = f'{client.s3}/companies_metadata/{now}'

    with client.get_connection() as conn:
        conn.sql(f"""
        COPY (SELECT *, CURRENT_DATE() as date, FROM companies_metadata)
        TO '{path}.parquet' (FORMAT PARQUET)
        """)

@dg.asset(retry_policy=API_RETRY_POLICY,
          deps=[wig20_companies_metadata])
def daily_ohlc(context: dg.AssetExecutionContext, ducks3: DuckDBS3Resource) -> None:
    """daily ohlc + volume data for WIG20 companies"""
    client = ducks3.get_resource()
    gpw = GpwSource()
    with client.get_connection() as conn:
        current_isins = client.get_latest_isins()
        today = datetime.datetime.now()
        ohlc_dfs_list = [gpw.fetch_ohlc(isin) for isin in current_isins]
        ohlc = pl.concat(ohlc_dfs_list, how='vertical_relaxed')
        ohlc = ohlc.with_columns(pl.lit(today).alias('date'))
        path = f"{client.s3}/ohlc"
        with client.get_connection() as conn:
            conn.sql(f"""
            COPY (SELECT *, YEAR(date) as year, MONTH(date) as month, DAY(date) as day
            FROM ohlc)  TO '{path}' 
            (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE)
            """)


@dg.asset(retry_policy=API_RETRY_POLICY)
def historical_ohlc(context: dg.AssetExecutionContext, ducks3: DuckDBS3Resource) -> None:
    """Historical daily ohlc data for every company, starting of each debut date to today

        - materialize via UI if needed
        - use for initial setup
    """
    client = ducks3.get_resource()
    gpw = GpwSource()
    isins = gpw.fetch_all_wig20_isin()
    history_ohlc = pl.concat([gpw.fetch_company_history_data(isin) for isin in isins])
    now = datetime.datetime.now().isoformat()
    client.write_data(history_ohlc, f'/ohlc_seed/{now}.parquet')

