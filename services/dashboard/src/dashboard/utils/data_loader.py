import os
import streamlit as st
import requests as req
import polars as pl
from datetime import timedelta, date

api = os.environ.get('API_URL')
pl.DataFrame()


@st.cache_resource(ttl=timedelta(days=30))
def load_companies_meta():
    response = req.get(f"{api}/company/")
    response.raise_for_status()
    response.close()
    response.close()
    return pl.DataFrame(response.json())


@st.cache_resource
def load_ohlc_daily(cache_key: date):
    """load daily aggregated ohlc data since debut on the stock market"""

    response = req.get(f'{api}/ohlc')
    response.close()
    ohlc_data = pl.read_parquet(response.content)
    return ohlc_data


@st.cache_resource(ttl=timedelta(minutes=15))
def load_today_ohlc_minutely():
    """load data for today/last day of ohlc data. Loaded every 15 min."""
    endpoint = f'/ohlc?mode=minutely'
    response = req.get(f"{api}/{endpoint}")
    response.close()
    ohlc_data = pl.read_parquet(response.content)
    return ohlc_data


@st.cache_resource
def load_news_to_yesterday(key: str):
    """Load all news from the beginning to yesterday, loaded once a day.
    Historical news doesn't change, so we fetch it once daily.
    Using today's date as cache key ensures automatic invalidation at midnight.
    This is the expensive operation (~360 files after 6 months), so aggressive
    caching is critical for dashboard responsiveness."""

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    endpoint = f"{api}/news?date_to={yesterday}"

    # Fetching all news (~15MB/year) and filtering client-side is faster
    # and simpler than maintaining filter logic in API for this data volume
    response = req.get(endpoint)
    response.close()
    news = pl.read_parquet(response.content)
    news = _filter_news(news, True)
    return news


def _filter_news(news: pl.DataFrame, filter_by_keywords: bool) -> pl.DataFrame:
    filter_keywords = [
        'giełd', 'gpw', 'kurs', 'akcj', 'notowa',
        'wig', 'sesj', 'hand', 'inwestor', 'spół',
        'wynik', 'raport', 'zysk', 'przychod', 'przychód', 'dochód', 'dochod', 'kontrakt',
        'dolar', 'euro', 'frank', 'funt'
    ]
    filter_keywords = r'\b|'.join(filter_keywords)

    filter_expression = pl.col('company_isins').list.len() > 0

    if filter_by_keywords:
        filter_expression = (filter_expression |
                             pl.col('title').str.contains("(?i)" + filter_keywords) |
                             pl.col('summary').str.contains("(?i)" + filter_keywords)
                             )

    news = news.filter(filter_expression).sort('date', descending=True)
    return news


@st.cache_resource(ttl=timedelta(minutes=60))
def load_all_news() -> pl.DataFrame:
    """Append today news to previous and filter to get only relevant news."""
    today = date.today().isoformat()
    to_yesterday = load_news_to_yesterday(today)

    today_news_response = req.get(f"{api}/news?date_from={today}")
    today_news_response.close()
    today_news = pl.read_parquet(today_news_response.content)
    today_news = _filter_news(today_news, True)
    news = pl.concat([to_yesterday, today_news], how='vertical_relaxed')

    return news


@st.cache_resource(ttl=timedelta(hours=12))
def load_currencies(curr_type='mid_market_rate', currencies_list: list[str] = None):
    currencies_data = []
    if currencies_list:
        for curr in currencies_list:
            response = req.get(f"{api}/currencies?curr_type={curr_type}&curr_code={curr}")
            response.close()
            currencies_data.append(pl.read_parquet(response.content))
        return pl.concat(currencies_data, how='vertical_relaxed')
    else:
        response = req.get(f"{api}/currencies?curr_type={curr_type}")
        response.close()
        return pl.read_parquet(response.content)


@st.cache_resource(ttl=timedelta(hours=12))
def load_gold_prices():
    response = req.get(f"{api}/gold")
    response.close()
    return pl.read_parquet(response.content)


@st.cache_data(ttl=timedelta(hours=1))
def load_llm_summary():
    response = req.get(f'{api}/llm_summary')
    response.close()
    return response.json()
