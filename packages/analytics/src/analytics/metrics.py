import polars as pl
import duckdb


def calculate_gold_changes(gold_prices: pl.DataFrame, limit:int|None = 1) -> pl.DataFrame:
    """Return DataFrame which contains daily % change of gold prices"""
    gold_prices = duckdb.sql(f"""
          WITH lagged AS (
            SELECT *, 
                LAG(price) OVER(ORDER BY date) as prev_price
            FROM gold_prices
        )
        SELECT *,
            CASE WHEN (price - prev_price) > 0 THEN TRUE ELSE FALSE END as is_rise,
            (price - prev_price) / prev_price * 100 as change
        FROM lagged
        ORDER BY date DESC
        {'LIMIT ' + str(limit) if limit else ''}
        """).pl()
    return gold_prices


def calculate_currencies_changes(currencies: pl.DataFrame) -> pl.DataFrame:
    """Calculate the percentage change in exchange rates for popular currencies over the past day."""
    currencies = duckdb.sql(f"""
            WITH lagged AS (
            SELECT *, LAG(mid) OVER(PARTITION BY code ORDER BY effective_date) as prev_mid
            FROM currencies
            )
            SELECT *, CASE WHEN mid - prev_mid > 0
                THEN TRUE
                ELSE FALSE END as is_rise,
                ROUND((mid - prev_mid) / prev_mid * 100, 4) as change

            FROM lagged
            ORDER BY effective_date DESC, code
    """).pl()
    return currencies


def calculate_daily_stock_performance(ohlc_daily: pl.DataFrame, companies_meta: pl.DataFrame) \
        -> tuple[pl.DataFrame, pl.DataFrame]:
    """Calculates the percentage change for each stock over the past day and returns gainers and losers."""
    query_daily_change = """
                         SELECT isin,
                                date,
                                close,
                                LAG(close) OVER (PARTITION by isin ORDER BY date) as prev_close,
                                ROUND((close - prev_close) / prev_close * 100, 4) as change
                         FROM ohlc_daily QUALIFY ROW_NUMBER() OVER (PARTITION BY isin ORDER BY date DESC) = 1
                         """

    with duckdb.connect(":memory:"):
        movers = duckdb.sql(query_daily_change).pl()
    movers = (movers.join(companies_meta, left_on='isin', right_on='company_isin')
              .with_columns(
        type=pl.when(pl.col('change') > 0).
        then(pl.lit('gainer')).
        otherwise(pl.lit('loser'))
    ).sort('date', descending=True)
              )

    gainers = (movers.filter(pl.col('type') == "gainer").
               sort('change', descending=True)
               )
    losers = (movers.filter(pl.col('type') == 'loser').
              sort('change')
              )

    return gainers, losers
