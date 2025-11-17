import dagster as dg

news_update = dg.define_asset_job(
    name="news_update",
    selection=dg.AssetSelection.groups('news')
)

currency_daily = dg.define_asset_job(
    name="currency_daily",
    description="Popular currencies rates",
    selection=dg.AssetSelection.assets("currencies_today_bid_ask",
                                       "currencies_today_mid_market_rate")
)

currency_unpopular = dg.define_asset_job(
    name="currency_unpopular",
    description="Unpopular currencies",
    selection=dg.AssetSelection.assets("currencies_today_mid_market_rate_unpopular")
)

wig20_metadata = dg.define_asset_job(
    name="wig20_metadata",
    description="Metadata of current WIG20 companies",
    selection=dg.AssetSelection.assets("wig20_companies_metadata")
)

ohlc_job = dg.define_asset_job(
    name="ohlc_today",
    description="Daily OHLC data for WIG20",
    selection=dg.AssetSelection.assets("daily_ohlc")
)

gemini_job = dg.define_asset_job(
    name="gemini_summary_job",
    description="LLM daily summary of stock market news",
    selection=dg.AssetSelection.assets('gemini_daily_summary')
)

gold_job = dg.define_asset_job(
    name='gold_daily_job',
    description="Daily gold price scraping job",
    selection=dg.AssetSelection.assets("gold_prices")
)
