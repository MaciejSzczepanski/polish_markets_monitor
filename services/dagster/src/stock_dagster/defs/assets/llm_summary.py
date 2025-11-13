import logging

import dagster as dg
from stock_dagster.config import API_RETRY_POLICY
from stock_dagster.defs.resources import GeminiResource, DuckDBS3Resource
import polars as pl
from datetime import date, datetime
from analytics.metrics import calculate_daily_stock_performance, calculate_currencies_changes, calculate_gold_changes

logger = logging.getLogger(__name__)


def format_important_currency_movers(currency: pl.DataFrame, gold: pl.DataFrame,
                                     currencies: tuple = ('USD', 'EUR', 'GBP', 'CHF', 'RUB', 'CNY', 'JPY')):
    """format top movers to summarize for LLM"""
    currency = (currency.filter(pl.col('code').is_in(currencies))
                .sort('effective_date', descending=True).head(len(currencies))
                )
    summary = []
    for row in currency.iter_rows(named=True):
        if abs(row['change']) >= 0.5:
            # if change is greater than 0.5% we consider it significant
            summary.append(
                f"{row['currency']}- cena: {row['mid']:2f} PLN, wzrost/spadek(%): {row['change']:2f}%"
            )
    gold = calculate_gold_changes(gold).filter(pl.col('date') == date.today()).row(0, named=True)
    summary.append(f"Zloto: cena: {gold['price']:.2f} PLN, wzrost/spadek(%): {gold['change']:.2f}%")
    return '\n'.join(summary)


def format_gainers_losers(daily_ohlc: pl.DataFrame, meta: pl.DataFrame):
    """Format top gainers and losers for summarization."""
    today = date.today()
    daily_ohlc = daily_ohlc.filter(pl.col('date') >= pl.lit(today).dt.offset_by('-3d'))

    gainers, losers = calculate_daily_stock_performance(daily_ohlc, meta)
    movers = (pl.concat([gainers, losers])
              .filter(pl.col('date') == today)
              .sort('change', descending=True))

    summary = []
    for mover in movers.iter_rows(named=True):
        summary.append(
            f"{mover['name']} cena: {mover['close']}, zmiana: {mover['change']:.2f}%"
        )
    return '\n'.join(summary)


def format_news_titles(news: pl.DataFrame):
    news_formated = []
    for row in news.iter_rows(named=True):
        news_formated.append(f"""- {row['title']}  
podsumowanie: {row['summary']}  """)

    return '\n'.join(news_formated)


def prepare_prompt(ducks3: DuckDBS3Resource):
    """Prepare context for LLM"""
    client = ducks3.get_resource()
    today = date.today().isoformat()

    meta = client.get_companies_metadata()
    gold = client.get_gold_prices()
    ohlc_daily = client.aggregate_ohlc_daily()
    stocks_summary = format_gainers_losers(ohlc_daily, meta)

    currency_data = client.get_currencies("mid_market_rate")
    currency_data = calculate_currencies_changes(currency_data)
    currency = format_important_currency_movers(currency_data, gold)

    news = client.get_news(date_from=today)
    news = format_news_titles(news)
    prompt = f"""
    ## ZADANIE
    Podsumuj dzisiejsze wydarzenia na rynku finansowym w 3-4 zwięzłych punktach (po polsku).

    ## DANE WEJŚCIOWE
    Data: {today}

    ### WIG20 (zmiany cen)
    {stocks_summary}  

    ### Waluty i Złoto
    {'Znaczące ruchy walutowe:' if currency else 'Brak znaczących ruchów.'}
    {currency}  

    ### Najważniejsze Newsy
    {news}

    ## INSTRUKCJE I REGUŁY
    - Zignoruj dane, jeśli są puste lub nieistotne (np. 'Brak znaczących ruchów').
    - Maksymalnie 5 punktów.
    - Priorytet: akcje (GPW) i newsy o WIG20, potem waluty (jeśli są duże zmiany >0.5%).
    - Każdy punkt to maksymalnie 2 zdania.
    - Skup się na: głównych ruchach cenowych, ważnych newsach firm, ogólnym trendzie rynku.
    {"- Jeśli są znaczące ruchy walutowe (>0.5%), wspomnij je krótko." if currency else ""}

    ## FORMAT WYJŚCIOWY
    Zwróć odpowiedź **tylko i wyłącznie** w formacie markdown, zaczynając od tytułu H3. Nie dodawaj żadnych wstępów ani komentarzy.

    ### Podsumowanie Rynku: {today}
    - [Pierwszy punkt podsumowania]
    - [Drugi punkt podsumowania]
    - ...
    """
    return prompt


@dg.asset(retry_policy=API_RETRY_POLICY
          )
def gemini_daily_summary(gemini: GeminiResource, ducks3: DuckDBS3Resource) -> None:
    prompt = prepare_prompt(ducks3)
    response = gemini.send_request(prompt)
    now = datetime.now()
    summary = {'date': now,
               'year': now.year,
               'month': now.month,
               'summary': response}
    summary = pl.DataFrame(summary)
    client = ducks3.get_resource()
    with client.get_connection() as conn:
        conn.sql(
            f"""COPY summary to '{client.s3}/llm_summaries/' 
             (FORMAT PARQUET, PARTITION_BY (year, month), APPEND)""")

