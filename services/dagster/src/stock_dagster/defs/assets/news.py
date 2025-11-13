import datetime
from typing import Protocol, Type

import polars as pl
from polars import DataFrame
import dagster as dg
from stock_dagster.config import API_RETRY_POLICY

from data_sources.sources.bankier.client import BankierSource
from data_sources.sources.biznes_interia.client import InteriaSource
from ...defs.resources import DuckDBS3Resource


class NewsSource(Protocol):
    name: str

    def fetch_news(self) -> DataFrame:
        pass


def _rss_template(context: dg.AssetExecutionContext, ducks3: DuckDBS3Resource, source: Type[NewsSource]):
    client = ducks3.get_resource()
    source = source()
    news = source.fetch_news()
    # ensure column 'date' is in date type
    news = news.with_columns(pl.col('date').dt.date())
    now = datetime.datetime.now()

    path = f'/news/year={now.year}/month={now.month}/day={now.day}/{source.name.title()}Source.parquet'
    with client.get_connection() as conn:
        if client.file_exists(path):
            news_fetched_today = client.get_today_news()
            # ensuring that date column is date type
            news_fetched_today = news_fetched_today.with_columns(pl.col('date').dt.date())
            news = pl.concat([news_fetched_today, news])
            news = news.unique(subset='link')

        client.write_data(news, path)


def build_news_asset(source: Type[NewsSource]):
    @dg.asset(group_name='news', name=f'news_{source.__name__}',
              retry_policy=API_RETRY_POLICY)
    def _asset(context: dg.AssetExecutionContext, ducks3: DuckDBS3Resource):
        return _rss_template(context, ducks3, source)

    return _asset


bankier_news = build_news_asset(BankierSource)
interia_news = build_news_asset(InteriaSource)
