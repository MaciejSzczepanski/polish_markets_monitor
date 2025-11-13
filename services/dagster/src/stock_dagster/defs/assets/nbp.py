import datetime
import dagster as dg
from dagster import AssetsDefinition
from stock_dagster.config import API_RETRY_POLICY
from ...utils import build_date_intervals_df
from data_sources.sources.nbp.client import NbpSource
from data_sources.sources.nbp.client import CurrencyType
from ...defs.resources import DuckDBS3Resource
import polars as pl


def _currency_today(curr_type: CurrencyType, ducks3: DuckDBS3Resource, context: dg.AssetExecutionContext) -> None:
    """ generate template of asset for various currency types"""
    client = ducks3.get_resource()
    nbp = NbpSource()
    currency_data_today = nbp.fetch_currencies_today(curr_type)
    currency_today_df = nbp.transform_currency(currency_data_today)

    # drop for compatibility with data fetched by _currency_today; mid_market_rate_unpopular has inconsistent
    # schema for today and past data
    if curr_type == "mid_market_rate_unpopular":
        currency_today_df.drop('country', strict=False)

    path = f"/currencies/{curr_type}.parquet"
    currency_past = client.read_file(path)

    currency_concated = pl.concat([currency_past, currency_today_df], how='vertical_relaxed')
    client.write_data(currency_concated, path)


def build_currency_asset(curr_type: CurrencyType) -> AssetsDefinition:
    """Factory function for building currency asset for various currency types"""

    @dg.asset(name=f'currencies_today_{curr_type}', group_name='nbp',
              retry_policy=API_RETRY_POLICY)
    def _asset(context: dg.AssetExecutionContext, ducks3: DuckDBS3Resource):
        return _currency_today(curr_type, ducks3, context)

    return _asset


def _currency_backfill(curr_type: CurrencyType,
                       ducks3: DuckDBS3Resource,
                       context: dg.AssetExecutionContext) -> None:
    """template for backfilling currency data"""
    client = ducks3.get_resource()
    nbp = NbpSource()
    date_intervals = build_date_intervals_df(start_date=datetime.date(2012, 1, 2),
                                             end_date=datetime.date.today(),
                                             interval='3mo')

    backfill_dfs = []
    for di in date_intervals.to_dicts():
        backfill_df = nbp.fetch_currencies_daterange(curr_type=curr_type, date_from=di.get('start'),
                                                     date_to=di.get('end'))
        backfill_df = nbp.transform_currency(backfill_df)
        # removing inconsistency, most data has no country full text representation
        backfill_df = backfill_df.drop('country', strict=False)
        backfill_dfs.append(backfill_df)
    backfill_concated = pl.concat(backfill_dfs, how='diagonal_relaxed')

    path = f"/currencies/{curr_type}.parquet"
    client.write_data(backfill_concated, path)


def build_currency_backfill(curr_type: CurrencyType) -> AssetsDefinition:
    @dg.asset(name=f'currencies_backfill_{curr_type}', group_name='nbp',
              retry_policy=API_RETRY_POLICY)
    def _asset(context: dg.AssetExecutionContext, ducks3: DuckDBS3Resource):
        return _currency_backfill(curr_type, ducks3, context)

    return _asset


@dg.asset(group_name='nbp', retry_policy=API_RETRY_POLICY)
def gold_prices(context: dg.AssetExecutionContext, ducks3: DuckDBS3Resource) -> None:
    """ gold prices history - update data, if data doesn't exist - extract all"""
    client = ducks3.get_resource()
    nbp = NbpSource()
    colnames = ['date', 'price']

    if client.file_exists('/gold_prices/gold_prices.parquet'):
        today_price = nbp.fetch_gold_actual()
        today_price = pl.DataFrame(today_price.json())
        today_price.columns = colnames

        prices = client.read_file('/gold_prices/gold_prices.parquet')
        prices = pl.concat([prices, today_price])
        client.write_data(prices, '/gold_prices/gold_prices.parquet')

    else:
        # file doesn't exist, backfilling
        date_intervals = build_date_intervals_df(datetime.date(2013, 1, 1), datetime.date.today(),
                                                 interval='3mo')

        gold_prices_intervals = []
        for di in date_intervals.to_dicts():
            gold_prices_intervals.append(
                pl.DataFrame(
                    nbp.fetch_gold_datarange(date_from=di.get('start'), date_to=di.get('end')).
                    json()
                )
            )
        gold_prices_concated = pl.concat(gold_prices_intervals, how='vertical_relaxed')
        gold_prices_concated.columns = colnames
        client.write_data(gold_prices_concated, f'/gold_prices/gold_prices.parquet')


currencies_today_bid_ask = build_currency_asset('bid_ask')
currencies_today_mid_market_rate = build_currency_asset('mid_market_rate')
currencies_today_bid_market_rate_unpopular = build_currency_asset('mid_market_rate_unpopular')

currencies_backfill_bid_ask = build_currency_backfill('bid_ask')
currencies_backfill_mid_market_rate = build_currency_backfill('mid_market_rate')
currencies_backfill_unpopular = build_currency_backfill('mid_market_rate_unpopular')
